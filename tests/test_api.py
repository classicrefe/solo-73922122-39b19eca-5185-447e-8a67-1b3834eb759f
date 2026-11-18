"""端到端自测：覆盖分页、快照隔离、名额、关闭、到期、连接丢失与重启语义。

运行方式（本地已起好 PostgreSQL 15，见 README）：
    DATABASE_URL=postgresql://actuary:actuary@127.0.0.1:55432/claims \
    SESSION_TTL_SECONDS=2 ENABLE_DEBUG_HOOKS=true \
    python -m unittest tests.test_api -v
"""
from __future__ import annotations

import concurrent.futures
import os
import threading
import time
import unittest

import httpx
import psycopg
import uvicorn

# 本地测试用默认值
os.environ.setdefault(
    "DATABASE_URL", "postgresql://actuary:actuary@127.0.0.1:55432/claims"
)
os.environ.setdefault("SESSION_TTL_SECONDS", "2")
os.environ.setdefault("ENABLE_DEBUG_HOOKS", "true")
os.environ.setdefault("SWEEPER_INTERVAL_SECONDS", "0.1")

# 测试端口可用 TEST_PORT 覆盖，避免与环境中残留的实例冲突
TEST_PORT = int(os.environ.get("TEST_PORT", "18080"))

from app.config import settings  # noqa: E402
from app.main import app, manager  # noqa: E402
from app.session_manager import SessionError  # noqa: E402

SEED_SQL = os.path.join(os.path.dirname(__file__), "..", "db", "init", "02_seed.sql")


def reset_seed_data() -> None:
    """每个用例前重置演示数据，保证改/删类测试可重复运行。

    用 DELETE 取代种子脚本里的 TRUNCATE：DELETE 只取 RowExclusiveLock，
    与快照会话持有的 AccessShareLock 兼容（MVCC），不会因环境中残留的
    只读事务而排队；序列重置交给种子脚本末尾的 setval 完成。
    """
    with open(SEED_SQL, encoding="utf-8") as fh:
        sql = fh.read()
    sql = sql.replace(
        "TRUNCATE claims, policies RESTART IDENTITY CASCADE;",
        "DELETE FROM claims; DELETE FROM policies;",
    )
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute(sql)

TTL = settings.session_ttl_seconds


class ApiTestBase(unittest.TestCase):
    # 整个套件共享一个 uvicorn 实例与一个 manager 单例，
    # 避免多实例争抢同一端口导致请求串到旧进程。
    _server: "uvicorn.Server | None" = None
    _server_thread: threading.Thread | None = None
    client: httpx.Client

    @classmethod
    def setUpClass(cls):
        if ApiTestBase._server is None:
            config = uvicorn.Config(
                "app.main:app", host="127.0.0.1", port=TEST_PORT,
                log_level="warning",
            )
            ApiTestBase._server = uvicorn.Server(config)
            ApiTestBase._server_thread = threading.Thread(
                target=ApiTestBase._server.run,
                name="test-uvicorn", daemon=True,
            )
            ApiTestBase._server_thread.start()
            for _ in range(100):
                try:
                    httpx.get(
                        f"http://127.0.0.1:{TEST_PORT}/health", timeout=1
                    )
                    break
                except httpx.TransportError:
                    time.sleep(0.1)
            # 全套件以干净的种子数据开始
            reset_seed_data()
        cls.client = httpx.Client(
            base_url=f"http://127.0.0.1:{TEST_PORT}", timeout=15
        )

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def setUp(self):
        # 每个用例前清掉所有未终结会话，保证相互独立
        for s in manager.list_sessions():
            if s.status in ("open", "closing"):
                try:
                    manager.close(s.session_id)
                except SessionError:
                    pass
        # 写入类用例会改数据，统一在用例内自行安排快照时机，
        # 不在全局 setUp 中重置（否则会清掉同用例刚提交的写入）。

    @classmethod
    def _reset_seed(cls):
        reset_seed_data()

    def open(self, region="华北"):
        r = self.client.post("/sessions", json={"region": region})
        self.assertEqual(r.status_code, 201, r.text)
        return r.json()

    def close(self, sid):
        return self.client.post(f"/sessions/{sid}/close")

    def page(self, sid, page=1, page_size=100, headers=None):
        return self.client.get(
            f"/sessions/{sid}/claims",
            params={"page": page, "page_size": page_size},
            headers=headers or {},
        )

    def correct(self, sid, corrections, headers=None):
        return self.client.post(
            "/claim-corrections",
            json={"session_id": sid, "corrections": corrections},
            headers=headers or {},
        )

    def source_amounts(self, claim_ids):
        """直接读源库当前已提交的金额，绕过快照会话。"""
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            rows = conn.execute(
                "SELECT id, amount FROM claims WHERE id = ANY(%s)",
                (list(claim_ids),),
            ).fetchall()
        return {cid: amt for cid, amt in rows}

    def source_row(self, claim_id):
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            return conn.execute(
                """
                SELECT c.id, c.policy_id, p.region, c.amount
                  FROM claims c JOIN policies p ON p.id = c.policy_id
                 WHERE c.id = %s
                """,
                (claim_id,),
            ).fetchone()


class TestPagination(ApiTestBase):
    def test_pages_ordered_and_capped(self):
        s = self.open()
        r1 = self.page(s["session_id"])
        self.assertEqual(r1.status_code, 200)
        body = r1.json()
        self.assertEqual(len(body["rows"]), 100)
        self.assertTrue(body["has_more"])
        ids = [r["claim_id"] for r in body["rows"]]
        self.assertEqual(ids, sorted(ids))
        self.assertTrue(all(r["region"] == "华北" for r in body["rows"]))
        self.assertTrue(all(isinstance(r["amount"], int) for r in body["rows"]))

        r2 = self.page(s["session_id"], page=2)
        self.assertEqual(len(r2.json()["rows"]), 20)
        self.assertFalse(r2.json()["has_more"])
        self.assertGreater(
            r2.json()["rows"][0]["claim_id"], body["rows"][-1]["claim_id"]
        )

        r3 = self.page(s["session_id"], page=3)
        self.assertEqual(r3.json()["rows"], [])
        self.assertFalse(r3.json()["has_more"])

    def test_page_size_forced_to_cap(self):
        s = self.open()
        r = self.page(s["session_id"], page_size=999)
        self.assertEqual(r.json()["page_size"], 100)
        self.assertEqual(len(r.json()["rows"]), 100)

    def test_invalid_pagination_params(self):
        s = self.open()
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page": 0}
        ).status_code, 422)
        self.assertEqual(self.client.get(
            f"/sessions/{s['session_id']}/claims", params={"page_size": 0}
        ).status_code, 422)

    def test_repeat_page_same_snapshot(self):
        s = self.open()
        b1 = self.page(s["session_id"]).json()
        b2 = self.page(s["session_id"]).json()
        self.assertEqual(
            [r["claim_id"] for r in b1["rows"]],
            [r["claim_id"] for r in b2["rows"]],
        )
        self.assertEqual(b1["snapshot_time"], b2["snapshot_time"])


class TestSnapshotIsolation(ApiTestBase):
    def test_source_writes_invisible_inside_session(self):
        self._reset_seed()  # 本用例需要干净的种子数据
        s = self.open("华北")
        before = self.page(s["session_id"]).json()
        before_ids = {r["claim_id"] for r in before["rows"]}

        # 源端：新增保单+理赔、改金额、删理赔，均独立提交
        pol = self.client.post("/admin/policies", json={"region": "华北"})
        self.assertEqual(pol.status_code, 201)
        nc = self.client.post(
            "/admin/claims",
            json={"policy_id": pol.json()["id"], "amount": 777},
        )
        self.assertEqual(nc.status_code, 201)
        first_id = before["rows"][0]["claim_id"]
        second_id = before["rows"][1]["claim_id"]
        self.assertEqual(self.client.patch(
            f"/admin/claims/{first_id}", params={"amount": 1}
        ).status_code, 200)
        self.assertEqual(self.client.delete(
            f"/admin/claims/{second_id}"
        ).status_code, 204)

        after = self.page(s["session_id"]).json()
        after_ids = {r["claim_id"] for r in after["rows"]}
        self.assertEqual(before_ids, after_ids)
        self.assertNotIn(nc.json()["id"], after_ids)
        self.assertNotEqual(after["rows"][0]["amount"], 1)
        self.assertIn(second_id, after_ids)

        # 新会话能看到源端最新提交（翻完全部页核对）
        s2 = self.open("华北")
        fresh_ids = set()
        page_no = 1
        while True:
            body = self.page(s2["session_id"], page=page_no).json()
            fresh_ids.update(r["claim_id"] for r in body["rows"])
            if not body["has_more"]:
                break
            page_no += 1
        self.assertIn(nc.json()["id"], fresh_ids)
        self.assertNotIn(second_id, fresh_ids)
        self.close(s2["session_id"])

    def test_snapshot_covers_both_tables(self):
        # 地区来自 policies 表、金额来自 claims 表：JOIN 结果同样固定
        s = self.open("华东")
        b = self.page(s["session_id"]).json()
        self.assertEqual(len(b["rows"]), 100)
        self.assertTrue(all(r["region"] == "华东" for r in b["rows"]))


class TestCapacity(ApiTestBase):
    def test_max_two_sessions_and_release(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        r = self.client.post("/sessions", json={"region": "华北"})
        self.assertEqual(r.status_code, 429)
        self.assertEqual(r.json()["code"], "capacity_reached")

        # 满额不影响既有会话
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)

        self.assertEqual(self.close(s2["session_id"]).json()["status"], "closed")
        s3 = self.open("华北")  # 名额归还后可再开
        self.assertEqual(s3["status"], "open")
        # 另一个会话不受影响
        self.assertEqual(self.page(s1["session_id"], page_size=5).status_code, 200)
        self.close(s1["session_id"])
        self.close(s3["session_id"])

    def test_unknown_session_404(self):
        r = self.client.get("/sessions/nonexistent/claims")
        self.assertEqual(r.status_code, 404)
        self.assertEqual(self.client.get("/sessions/nonexistent").status_code, 404)


class TestCloseSemantics(ApiTestBase):
    def test_close_rejects_later_pages_but_keeps_status_queryable(self):
        s = self.open()
        self.close(s["session_id"])
        r = self.page(s["session_id"])
        self.assertEqual(r.status_code, 409)
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{s['session_id']}").json()
        self.assertEqual(info["status"], "closed")

    def test_slow_page_then_close(self):
        s = self.open("华东")
        sid = s["session_id"]
        box = {}

        def slow():
            box["r"] = self.client.get(
                f"/sessions/{sid}/claims",
                params={"page": 1, "page_size": 10},
                headers={"X-Debug-Sleep-Seconds": "3"},
                timeout=15,
            )

        t = threading.Thread(target=slow)
        t.start()
        time.sleep(0.8)
        r = self.close(sid)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["status"], "closed")
        t.join()
        self.assertIn(box["r"].status_code, (409, 410))
        self.assertNotIn("rows", box["r"].json())

        # 关闭后名额已归还，可开新会话
        s2 = self.open("华东")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])

    def test_close_idempotent_and_new_session_unaffected(self):
        s1 = self.open("华东")
        s2 = self.open("华南")
        self.close(s1["session_id"])
        again = self.close(s1["session_id"])
        self.assertEqual(again.json()["status"], "closed")
        # 另一个会话照常工作
        self.assertEqual(self.page(s2["session_id"], page_size=5).status_code, 200)
        self.close(s2["session_id"])


class TestExpiry(ApiTestBase):
    def test_expiry_rejects_pages_and_releases_slot(self):
        s = self.open("华南")
        sid = s["session_id"]
        time.sleep(TTL + 0.6)
        r = self.page(sid)
        self.assertIn(r.status_code, (409, 410))
        self.assertNotIn("rows", r.json())
        info = self.client.get(f"/sessions/{sid}").json()
        self.assertEqual(info["status"], "expired")
        # 名额已释放：能连开两个新会话
        a = self.open("华东")
        b = self.open("华北")
        self.assertEqual(a["status"], "open")
        self.assertEqual(b["status"], "open")
        self.close(a["session_id"])
        self.close(b["session_id"])

    def test_pages_before_expiry_still_work(self):
        s = self.open("华南")
        r = self.page(s["session_id"], page_size=10)
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(r.json()["rows"]), 10)


class TestConnectionLossAndRestart(ApiTestBase):
    """连接丢失（终止会话后端）与服务重启语义。

    本环境禁止 kill 系统调用，故用 pg_terminate_backend 从 SQL 层
    断开会话持有的后端连接，对客户端而言与网络/进程断开等价。
    """

    def _terminate_session_backend(self, sid: str) -> None:
        sess = manager.get_status(sid)
        with psycopg.connect(settings.database_url, autocommit=True) as admin:
            # 按客户端后端 PID 精准终止该会话独占的连接
            admin.execute(
                "SELECT pg_terminate_backend(%s)",
                (sess.conn.info.backend_pid,),
            )
        # 给客户端一点时间感知 TCP 断开
        deadline = time.time() + 5
        while time.time() < deadline:
            if sess.conn.closed:
                return
            time.sleep(0.1)

    def test_connection_loss_invalidates_session_no_snapshot_swap(self):
        s = self.open("华东")
        sid = s["session_id"]
        # 从 SQL 层断开会话持有的后端连接 -> 快照事务随之消失
        self._terminate_session_backend(sid)
        r = self.page(sid)
        # 旧会话直接失败，绝不换连接/换快照续取；
        # 409（清理线程先标记终态）或 503（取页时发现连接断裂）均合规
        self.assertIn(r.status_code, (409, 503))
        self.assertNotIn("rows", r.json())
        # 旧 id 无法续取（失效会话已移出注册表或已为终态）
        info = self.client.get(f"/sessions/{sid}")
        if info.status_code == 200:
            self.assertIn(info.json()["status"], ("closed", "expired"))
            self.assertEqual(self.page(sid).status_code, 409)
        else:
            self.assertEqual(info.status_code, 404)
        # 新会话在池恢复后的连接上正常开启
        s2 = self.open("华东")
        body = self.page(s2["session_id"], page_size=5)
        self.assertEqual(body.status_code, 200)
        self.assertEqual(len(body.json()["rows"]), 5)
        self.close(s2["session_id"])

    @staticmethod
    def _restart_uvicorn():
        """完整重启共享 uvicorn：lifespan 负责 manager.stop()/start()。"""
        ApiTestBase._server.should_exit = True
        ApiTestBase._server_thread.join(timeout=10)
        config = uvicorn.Config(
            "app.main:app", host="127.0.0.1", port=TEST_PORT, log_level="warning",
        )
        ApiTestBase._server = uvicorn.Server(config)
        ApiTestBase._server_thread = threading.Thread(
            target=ApiTestBase._server.run,
            name="test-uvicorn-restart", daemon=True,
        )
        ApiTestBase._server_thread.start()
        for _ in range(100):
            try:
                if httpx.get(
                    f"http://127.0.0.1:{TEST_PORT}/health", timeout=1
                ).status_code == 200:
                    return
            except httpx.TransportError:
                time.sleep(0.1)
        raise RuntimeError("uvicorn 重启后健康检查失败")

    def test_service_restart_drops_in_memory_sessions(self):
        s = self.open("华南")
        sid = s["session_id"]

        # 模拟 API 服务重启（新进程语义：内存注册表为空、池与 sweeper 重建）
        self._restart_uvicorn()

        # 旧会话 id 在新进程中不存在，不得换快照续取
        r = self.client.get(f"/sessions/{sid}")
        self.assertEqual(r.status_code, 404)
        r2 = self.page(sid)
        self.assertEqual(r2.status_code, 404)
        s2 = self.open("华南")
        self.assertEqual(s2["status"], "open")
        self.close(s2["session_id"])


class TestClaimCorrections(ApiTestBase):
    """POST /claim-corrections：快照核对、整批原子、源端防覆盖与并发。"""

    def setUp(self):
        super().setUp()
        reset_seed_data()  # 更正类用例会改金额，每个用例从干净种子开始

    def _two_huabei_claims(self, sid):
        """取会话首页前两条华北理赔，返回 (id, amount) 列表。"""
        body = self.page(sid, page_size=10).json()["rows"]
        return [(r["claim_id"], r["amount"]) for r in body[:2]]

    def test_happy_path_batch_updates_and_response_shape(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), (id2, amt2) = self._two_huabei_claims(sid)
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 500},
            {"claim_id": id2, "observed_amount": amt2, "new_amount": 0},
        ])
        self.assertEqual(r.status_code, 200, r.text)
        body = r.json()
        self.assertEqual(body["session_id"], sid)
        self.assertEqual(body["region"], "华北")
        self.assertEqual(body["updated_count"], 2)
        self.assertEqual(len(body["corrections"]), 2)
        self.assertEqual(body["corrections"][0],
                         {"claim_id": id1, "old_amount": amt1, "new_amount": amt1 + 500})
        self.assertEqual(body["corrections"][1],
                         {"claim_id": id2, "old_amount": amt2, "new_amount": 0})
        self.assertIn("committed_at", body)
        # 源库确实落库
        self.assertEqual(self.source_amounts([id1, id2]),
                         {id1: amt1 + 500, id2: 0})

    def test_old_session_keeps_old_amount_new_session_reads_new(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 777},
        ])
        self.assertEqual(r.status_code, 200, r.text)

        # 旧会话快照固定，仍读旧金额
        old_rows = {row["claim_id"]: row["amount"]
                    for row in self.page(sid, page_size=10).json()["rows"]}
        self.assertEqual(old_rows[id1], amt1)

        # 新会话读到更正值
        s2 = self.open("华北")
        new_rows = {row["claim_id"]: row["amount"]
                    for row in self.page(s2["session_id"], page_size=10).json()["rows"]}
        self.assertEqual(new_rows[id1], amt1 + 777)
        self.close(s2["session_id"])

    def test_snapshot_mismatch_wrong_observed_amount_rejects_all(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), (id2, amt2) = self._two_huabei_claims(sid)
        before = self.source_amounts([id1, id2])
        r = self.correct(sid, [
            # 所见金额故意填错
            {"claim_id": id1, "observed_amount": amt1 + 999, "new_amount": 1},
            {"claim_id": id2, "observed_amount": amt2, "new_amount": 2},
        ])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "snapshot_mismatch")
        self.assertIn(str(id1), r.json()["detail"])
        # 整批拒绝：两条都没动
        self.assertEqual(self.source_amounts([id1, id2]), before)

    def test_snapshot_mismatch_claim_from_other_region(self):
        s = self.open("华北")
        sid = s["session_id"]
        # 华东的理赔不在华北会话快照里
        east = self.client.post("/sessions", json={"region": "华东"})
        east_row = self.page(east.json()["session_id"], page_size=1).json()["rows"][0]
        self.close(east.json()["session_id"])
        r = self.correct(sid, [{
            "claim_id": east_row["claim_id"],
            "observed_amount": east_row["amount"],
            "new_amount": east_row["amount"] + 1,
        }])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "snapshot_mismatch")

    def test_snapshot_mismatch_unknown_claim_id(self):
        s = self.open("华北")
        r = self.correct(s["session_id"], [{
            "claim_id": 99999999, "observed_amount": 1, "new_amount": 2,
        }])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "snapshot_mismatch")

    def test_request_validation_duplicates_and_amount_rules(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        # 重复 id
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 1},
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 2},
        ])
        self.assertEqual(r.status_code, 422)
        # 新金额与所见相同
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1},
        ])
        self.assertEqual(r.status_code, 422)
        # 负数金额
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": -1},
        ])
        self.assertEqual(r.status_code, 422)
        # 空批次
        r = self.client.post("/claim-corrections",
                             json={"session_id": sid, "corrections": []})
        self.assertEqual(r.status_code, 422)

    def test_batch_limit_is_twenty(self):
        s = self.open("华北")
        sid = s["session_id"]
        rows = self.page(sid, page_size=100).json()["rows"]
        twenty = [
            {"claim_id": r["claim_id"],
             "observed_amount": r["amount"],
             "new_amount": r["amount"] + 1}
            for r in rows[:20]
        ]
        r = self.correct(sid, twenty)
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["updated_count"], 20)

    def test_unknown_session_404(self):
        r = self.correct("no-such-session", [
            {"claim_id": 1, "observed_amount": 1, "new_amount": 2},
        ])
        self.assertEqual(r.status_code, 404)
        self.assertEqual(r.json()["code"], "session_not_found")

    def test_closed_session_cannot_start_batch(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        self.close(sid)
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 1},
        ])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "session_not_open")

    def test_expired_session_cannot_start_batch(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        time.sleep(TTL + 0.6)
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 1},
        ])
        self.assertIn(r.status_code, (409, 410))
        self.assertIn(r.json()["code"], ("session_not_open", "session_expired"))

    def test_source_amount_changed_by_other_writer_whole_batch_rolls_back(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), (id2, amt2) = self._two_huabei_claims(sid)
        # 别人通过源端接口抢先改了 id2（会话快照仍记录旧值）
        self.assertEqual(self.client.patch(
            f"/admin/claims/{id2}", params={"amount": amt2 + 31337}
        ).status_code, 200)
        before = self.source_amounts([id1, id2])
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 1},
            {"claim_id": id2, "observed_amount": amt2, "new_amount": amt2 + 2},
        ])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "source_changed")
        self.assertIn(str(id2), r.json()["detail"])
        # 整批回滚：id1 也不能被写入，源端他人改动保持不变
        self.assertEqual(self.source_amounts([id1, id2]), before)

    def test_source_policy_reassignment_detected(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        # 源端把理赔挂到华东保单下（归属/地区都变了）
        with psycopg.connect(settings.database_url, autocommit=True) as conn:
            conn.execute("UPDATE claims SET policy_id = 3 WHERE id = %s", (id1,))
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 1},
        ])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "source_changed")

    def test_source_claim_deleted_detected(self):
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        self.assertEqual(self.client.delete(f"/admin/claims/{id1}").status_code, 204)
        r = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1, "new_amount": amt1 + 1},
        ])
        self.assertEqual(r.status_code, 409)
        self.assertEqual(r.json()["code"], "source_changed")

    def test_concurrent_batches_same_claim_only_one_succeeds(self):
        """两个会话的并发批次改同一理赔：先提交者成功，后到者整批拒绝。"""
        s1 = self.open("华北")
        s2 = self.open("华北")
        sid1, sid2 = s1["session_id"], s2["session_id"]
        (id1, amt1), (id2, amt2) = self._two_huabei_claims(sid1)

        barrier = threading.Barrier(2)

        def call_correct(sid, new_delta, sleep_s):
            barrier.wait()
            return self.client.post(
                "/claim-corrections",
                json={"session_id": sid, "corrections": [
                    {"claim_id": id1, "observed_amount": amt1,
                     "new_amount": amt1 + new_delta},
                ]},
                headers={"X-Debug-Sleep-Seconds": str(sleep_s)},
                timeout=20,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(call_correct, sid1, 111, 1.5)
            f2 = pool.submit(call_correct, sid2, 222, 0.0)
            r1, r2 = f1.result(), f2.result()

        statuses = sorted((r1.status_code, r2.status_code))
        self.assertEqual(statuses, [200, 409], (r1.text, r2.text))
        winner = r1 if r1.status_code == 200 else r2
        loser = r2 if r1.status_code == 200 else r1
        self.assertEqual(loser.json()["code"], "source_changed")
        # 源库只保留胜出批次的值
        self.assertEqual(self.source_amounts([id1])[id1],
                         winner.json()["corrections"][0]["new_amount"])
        self.close(sid1)
        self.close(sid2)

    def test_concurrent_batches_different_claims_both_succeed(self):
        s1 = self.open("华北")
        s2 = self.open("华北")
        sid1, sid2 = s1["session_id"], s2["session_id"]
        (id1, amt1), (id2, amt2) = self._two_huabei_claims(sid1)
        barrier = threading.Barrier(2)

        def call_correct(sid, cid, amt, delta, sleep_s):
            barrier.wait()
            return self.client.post(
                "/claim-corrections",
                json={"session_id": sid, "corrections": [
                    {"claim_id": cid, "observed_amount": amt,
                     "new_amount": amt + delta},
                ]},
                headers={"X-Debug-Sleep-Seconds": str(sleep_s)},
                timeout=20,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(call_correct, sid1, id1, amt1, 111, 1.0)
            f2 = pool.submit(call_correct, sid2, id2, amt2, 222, 0.0)
            r1, r2 = f1.result(), f2.result()
        self.assertEqual(r1.status_code, 200, r1.text)
        self.assertEqual(r2.status_code, 200, r2.text)
        self.assertEqual(self.source_amounts([id1, id2]),
                         {id1: amt1 + 111, id2: amt2 + 222})
        self.close(sid1)
        self.close(sid2)

    def test_concurrent_overlapping_batches_whole_loser_rolls_back(self):
        """两批部分重叠（共享一个 id）：一批成功，另一批整批回滚。"""
        s1 = self.open("华北")
        s2 = self.open("华北")
        sid1, sid2 = s1["session_id"], s2["session_id"]
        rows = self.page(sid1, page_size=10).json()["rows"]
        id_a, amt_a = rows[2]["claim_id"], rows[2]["amount"]
        id_b, amt_b = rows[3]["claim_id"], rows[3]["amount"]
        barrier = threading.Barrier(2)

        def overlap(i):
            barrier.wait()
            if i == 0:
                # 双条批次故意变慢，让单条批次先拿到共享行的锁
                return self.correct(sid1, [
                    {"claim_id": id_a, "observed_amount": amt_a,
                     "new_amount": amt_a + 11},
                    {"claim_id": id_b, "observed_amount": amt_b,
                     "new_amount": amt_b + 11},
                ], headers={"X-Debug-Sleep-Seconds": "1.5"})
            return self.correct(sid2, [
                {"claim_id": id_b, "observed_amount": amt_b,
                 "new_amount": amt_b + 22},
            ])

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(overlap, range(2)))
        statuses = sorted(r.status_code for r in results)
        self.assertEqual(statuses, [200, 409], [r.text for r in results])
        winner = next(r for r in results if r.status_code == 200)
        loser = next(r for r in results if r.status_code == 409)
        self.assertEqual(loser.json()["code"], "source_changed")
        source = self.source_amounts([id_a, id_b])
        if len(winner.json()["corrections"]) == 2:
            # 双条批次胜出：两行都落库
            self.assertEqual(source, {id_a: amt_a + 11, id_b: amt_b + 11})
        else:
            # 单条批次胜出：仅 id_b 更新，id_a 保持原值（双条批次整批回滚）
            self.assertEqual(source, {id_a: amt_a, id_b: amt_b + 22})
        self.close(sid1)
        self.close(sid2)

    def test_close_waits_for_inflight_correction_then_releases_snapshot(self):
        """关闭与已开始批次交错：批次先确定提交，关闭才释放快照。"""
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        box = {}

        def slow_correct():
            box["t0"] = time.time()
            box["r"] = self.correct(
                sid,
                [{"claim_id": id1, "observed_amount": amt1,
                  "new_amount": amt1 + 4242}],
                headers={"X-Debug-Sleep-Seconds": "3"},
            )
            box["t1"] = time.time()

        t = threading.Thread(target=slow_correct)
        t.start()
        time.sleep(0.8)  # 确保批次已开始（快照核对完成、持有 io_lock）
        t_close0 = time.time()
        close_r = self.close(sid)
        t_close1 = time.time()
        t.join()
        # 批次已开始：允许提交成功
        self.assertEqual(box["r"].status_code, 200, box["r"].text)
        self.assertEqual(self.source_amounts([id1])[id1], amt1 + 4242)
        # 关闭在批次确定终态后才完成（等到了慢批次结束）
        self.assertGreaterEqual(t_close1 - t_close0, 1.5)
        self.assertEqual(close_r.status_code, 200)
        self.assertEqual(close_r.json()["status"], "closed")
        # 快照随后已释放：新批次/取页不能再开始
        again = self.correct(sid, [
            {"claim_id": id1, "observed_amount": amt1 + 4242,
             "new_amount": amt1 + 1},
        ])
        self.assertEqual(again.status_code, 409)
        # 名额已归还，可开新会话读到更正值
        s2 = self.open("华北")
        rows = {r["claim_id"]: r["amount"]
                for r in self.page(s2["session_id"], page_size=10).json()["rows"]}
        self.assertEqual(rows[id1], amt1 + 4242)
        self.close(s2["session_id"])

    def test_close_accepted_before_batch_starts_rejects_batch(self):
        """关闭先被受理（批次在 io_lock 排队）：批次不得开始、零写入。"""
        s = self.open("华北")
        sid = s["session_id"]
        (id1, amt1), _ = self._two_huabei_claims(sid)
        before = self.source_amounts([id1])
        box = {}

        def slow_page_then_correct():
            # 先用慢取页占住 io_lock，关闭在此期间受理；
            # 取页结束后更正批次才拿到锁，复核到 closing 必须拒绝。
            box["page"] = self.page(
                sid, page_size=5, headers={"X-Debug-Sleep-Seconds": "3"}
            )
            box["correct"] = self.correct(sid, [
                {"claim_id": id1, "observed_amount": amt1,
                 "new_amount": amt1 + 9090},
            ])

        t = threading.Thread(target=slow_page_then_correct)
        t.start()
        time.sleep(0.8)
        close_r = self.close(sid)
        t.join()
        self.assertEqual(close_r.json()["status"], "closed")
        self.assertIn(box["page"].status_code, (409, 410))
        self.assertEqual(box["correct"].status_code, 409)
        self.assertEqual(box["correct"].json()["code"], "session_not_open")
        self.assertEqual(self.source_amounts([id1]), before)

    def test_pagination_and_admin_writes_remain_unchanged(self):
        """新功能不破坏原有分页与源端写入路径。"""
        s = self.open("华南")
        sid = s["session_id"]
        body = self.page(sid, page_size=100).json()
        self.assertEqual(len(body["rows"]), 40)
        self.assertFalse(body["has_more"])
        pol = self.client.post("/admin/policies", json={"region": "华南"})
        self.assertEqual(pol.status_code, 201)
        nc = self.client.post(
            "/admin/claims",
            json={"policy_id": pol.json()["id"], "amount": 123},
        )
        self.assertEqual(nc.status_code, 201)
        # 快照内依旧 40 行，源端写入不进快照
        self.assertEqual(len(self.page(sid, page_size=100).json()["rows"]), 40)
        self.close(sid)


if __name__ == "__main__":
    unittest.main(verbosity=2)
