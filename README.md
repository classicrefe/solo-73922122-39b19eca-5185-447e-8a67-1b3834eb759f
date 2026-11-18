# 理赔明细分批提取服务（FastAPI + PostgreSQL）

保险精算离线测算场景：按地区开启**快照会话**，按理赔 id 升序分页提取明细。
会话期间保单/理赔两表固定为开启时刻的同一快照，源端的增改删照常提交、
互不影响；会话有数量上限与 TTL，可主动关闭，关闭/到期后未开始响应的取页
一律失败且不返回明细。

## 一、快速启动（Docker Compose）

```bash
docker compose up --build
```

- API：<http://localhost:8000>
- Swagger 交互文档：<http://localhost:8000/docs>
- OpenAPI 原文：<http://localhost:8000/openapi.json>
- PostgreSQL：localhost:55432（库 `claims`，用户/密码 `actuary/actuary`）

首次启动时 `db/init/` 下脚本按文件名顺序自动执行：

| 文件 | 作用 |
| --- | --- |
| `01_schema.sql` | 建表：`policies(id, region)`、`claims(id, policy_id, amount)`，金额为非负整数（单位：分） |
| `02_seed.sql` | 演示数据：3 个地区、260 条理赔（华北 120 / 华东 100 / 华南 40） |
| `03_sql_examples.sql` | SQL 补录示例：新增保单/理赔、改金额、删除、按地区汇总 |

## 二、核心语义与实现方式

| 需求 | 实现 |
| --- | --- |
| 两表同一快照、后页/重复页一致 | 每会话独占一条连接，开启 `START TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`，连接保持 idle-in-transaction，快照事务全程不结束 |
| 源端增改删照常提交 | 写操作走连接池内其它自动提交连接，MVCC 下与快照只读事务互不阻塞 |
| 不预存结果全集 | 每次取页现场执行带 `LIMIT/OFFSET` 的 SQL，服务端不缓存明细 |
| claim_id 升序、每页至多 100 行 | `ORDER BY c.id ASC LIMIT n+1 OFFSET ...`，多取 1 行判断 `has_more`；`page_size` 服务端强制截断为 100 |
| 最多 2 个会话，满额拒绝 | 内存注册表 + 名额计数，超额返回 `429 capacity_reached`（可用 `MAX_SESSIONS` 调整） |
| 开启 60 秒到期 | 后台 sweeper 线程定时清理（`SESSION_TTL_SECONDS` 可调），到期取页返回 410/409 且无明细 |
| 主动关闭及时受理 | 关闭请求先把状态翻为 `closing`（新取页立即失败），等待在途取页结束后回滚事务、归还连接、释放名额，再回报 `closed` |
| 不影响另一会话/后续新会话 | 名额只统计 open/closing 会话；终态不占名额，连接归还池后立即可复用 |
| 连接丢失/服务重启后旧会话失效 | 会话状态只存进程内存；连接断裂的会话作废返回 503 且绝不换快照续取；重启后旧 id 查询 404 |

### 会话状态机

```
open ──主动关闭──▶ closing ──回滚事务/还连接/释放名额──▶ closed
  │                                                  ▲
  └──TTL 到期（sweeper）─────────────────────────── expired
```

终态记录保留 10 分钟供状态查询，之后从注册表回收；终态不占用会话名额。

## 三、API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sessions` | 按地区开启快照会话，body：`{"region": "华北"}` |
| GET | `/sessions` | 列出会话 |
| GET | `/sessions/{id}` | 查询会话状态 |
| POST | `/sessions/{id}/close` | 主动关闭 |
| GET | `/sessions/{id}/claims?page=1&page_size=100` | 快照内分页取理赔明细 |
| POST | `/claim-corrections` | 按会话快照整批更正理赔金额（最多 20 条，整批成功或整批拒绝） |
| POST | `/admin/policies` | 源端补录保单 |
| POST | `/admin/claims` | 源端补录理赔 |
| PATCH | `/admin/claims/{id}?amount=999` | 源端改金额 |
| DELETE | `/admin/claims/{id}` | 源端删除理赔 |
| GET | `/health`、`/meta` | 健康检查、服务参数 |

取页失败响应只含 `detail`/`code`，不含任何明细行：

```json
HTTP 409
{"detail": "会话 ... 关闭已受理或会话已结束，取页失败且不返回明细",
 "code": "session_not_open"}
```

### 理赔金额整批更正（POST /claim-corrections）

精算数据员从地区快照核出错录金额后，凭有效会话 id 一次性提交至多 20 条更正，
每条含 `claim_id`、快照中所见金额 `observed_amount`、新金额 `new_amount`。
服务端严格两阶段处理，**要么整批提交，要么整批拒绝，绝不留部分更正**：

1. **请求校验（422）**：批次 1..20 条；理赔 id 不得重复；新金额与所见金额都
   必须是非负整数且互不相同（Pydantic 在入口拦截）。
2. **快照核对（409 `snapshot_mismatch`）**：在会话自身的 RR 只读快照上逐条核对
   理赔是否存在、地区是否等于会话地区、快照金额是否等于所见金额；任一不符整批拒绝。
3. **源端复核与整批写入（409 `source_changed`）**：另取一条池连接开启独立写事务，
   `SELECT ... FOR UPDATE` 按 claim_id 升序锁定全部目标行，复核源库当前的
   **保单归属、地区、金额**仍与快照完全一致，再用单条 `UPDATE ... FROM (VALUES ...)`
   一次性更新整批并 `COMMIT`；任一不符立即 `ROLLBACK`。

并发与交错语义：

| 场景 | 行为 |
| --- | --- |
| 多批改同一理赔 | 固定加锁顺序避免死锁；先提交者成功，后到者复核到金额已变，整批 `source_changed` 回滚 |
| 多批改不同理赔 | 行锁互不冲突，均可成功提交 |
| 两批部分重叠 | 一批成功，另一批因共享行已变而整批回滚（其独有行也不写入） |
| 关闭/到期先受理 | 批次尚未开始即被拒（`session_not_open`/`session_expired`），零写入 |
| 批次已开始后关闭 | 关闭等待该批次确定提交或回滚（持有会话 `io_lock`），随后才回滚快照事务、释放连接与名额 |
| 更正成功后的可见性 | 旧会话快照固定、仍读旧金额；新会话开启后读到更正值（MVCC） |
| 快照连接失效 | 会话作废返回 503，绝不换快照续取，批次不提交 |

请求与响应示例：

```jsonc
// POST /claim-corrections
{
  "session_id": "a1b2c3...",
  "corrections": [
    {"claim_id": 12, "observed_amount": 800,  "new_amount": 900},
    {"claim_id": 15, "observed_amount": 1500, "new_amount": 0}
  ]
}
// 200
{
  "session_id": "a1b2c3...", "region": "华北", "updated_count": 2,
  "committed_at": "2026-11-18T03:56:07.812019Z",
  "corrections": [
    {"claim_id": 12, "old_amount": 800,  "new_amount": 900},
    {"claim_id": 15, "old_amount": 1500, "new_amount": 0}
  ]
}
```

> 调试：`ENABLE_DEBUG_HOOKS=true` 时，`X-Debug-Sleep-Seconds` 头同样作用于
> 更正批次（睡眠点在快照核对之后、源端写事务之前），用于演示"批次进行中关闭"。

## 四、慢取页演示

镜像默认 `ENABLE_DEBUG_HOOKS=true`，带 `X-Debug-Sleep-Seconds` 请求头
可让本次取页在执行 SQL 前先睡眠，用于演示"关闭及时受理"：

```bash
python3 demo/demo.py                 # 完整端到端演示（标准库即可运行）
BASE_URL=http://localhost:8000 python3 demo/demo.py
DEMO_TTL_WAIT=1 python3 demo/demo.py # 追加等待到期场景（约多花 1 分钟）
```

演示覆盖：分页与重复取页一致性、源端写入的快照隔离、名额满 429、
慢取页期间关闭（在途请求结束后失败、无明细）、到期失效。

## 五、本地自测（无 Docker 时）

仓库测试以真实 uvicorn + PostgreSQL 运行：

```bash
pip install -r requirements.txt
# 准备一个 PostgreSQL 15，并执行 db/init/01_schema.sql、02_seed.sql
DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:5432/claims \
SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
python -m unittest tests.test_api -v
```

34 个用例覆盖：分页/上限/参数校验、重复取页同快照、两表 JOIN 快照、
源端增改删不可见、名额与释放、关闭幂等、慢取页+关闭竞争、
到期清理、连接丢失作废不续取、服务重启旧会话 404；
以及整批更正的 19 个用例：成功落库与响应结构、旧/新会话可见性、
重复 id 与金额规则 422、快照不符（错金额/跨地区/不存在）、
源端金额改动/保单改挂/删除的整批回滚、20 条上限、关闭/到期拒入、
同行并发唯一成功、异行并发皆成功、部分重叠整批回滚、
更正与关闭的两种交错、原分页与源端写入不回归。

> 测试用例支持 `TEST_PORT` 环境变量覆盖内置 uvicorn 端口（默认 18080），
> 数据重置使用锁兼容的 `DELETE`（而非 `TRUNCATE`），可与在途快照事务并存。

## 六、配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://actuary:actuary@db:5432/claims` | 数据库连接串 |
| `MAX_SESSIONS` | `2` | 并发会话上限 |
| `SESSION_TTL_SECONDS` | `60` | 会话存活秒数 |
| `PAGE_SIZE_CAP` | `100` | 单页行数硬上限 |
| `ENABLE_DEBUG_HOOKS` | `false`（compose 中为 true） | 是否允许慢取页调试头 |
| `SWEEPER_INTERVAL_SECONDS` | `0.25` | 到期检查间隔 |

> 说明：服务固定单 uvicorn worker——会话注册表与名额是进程内状态，
> 多 worker 会各自持有独立状态，破坏会话语义。
