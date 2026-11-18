"""HTTP 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field, PositiveInt, model_validator


class SessionStatus(str, Enum):
    OPEN = "open"          # 可正常取页
    CLOSING = "closing"    # 关闭已受理，正在等待在途取页结束
    CLOSED = "closed"      # 事务已回滚、连接已归还、名额已释放
    EXPIRED = "expired"    # 到期后由清理线程转入的终态（语义同 closed）


class OpenSessionRequest(BaseModel):
    region: str = Field(..., min_length=1, max_length=64, description="保单地区")


class SessionInfo(BaseModel):
    session_id: str
    region: str
    status: SessionStatus
    opened_at: datetime
    expires_at: datetime
    last_page_at: datetime | None = None
    pages_served: int
    ttl_seconds: int


class ClaimRow(BaseModel):
    claim_id: int
    policy_id: int
    region: str
    amount: int


class ClaimPage(BaseModel):
    session_id: str
    region: str
    page: PositiveInt
    page_size: int
    has_more: bool
    snapshot_time: datetime
    rows: list[ClaimRow]


class PolicyIn(BaseModel):
    region: str = Field(..., min_length=1, max_length=64)


class PolicyOut(BaseModel):
    id: int
    region: str


class ClaimIn(BaseModel):
    policy_id: int
    amount: int = Field(..., ge=0, description="整数理赔金额，单位分")


class ClaimOut(BaseModel):
    id: int
    policy_id: int
    amount: int


# ----------------------------------------------------------------------
# 理赔金额整批更正（POST /claim-corrections）
# ----------------------------------------------------------------------
# 单批最多更正条数；快照核对、源端复核与整批写入都以此为上限。
CORRECTION_BATCH_LIMIT = 20


class ClaimCorrectionItem(BaseModel):
    """单条更正指令：理赔 id、地区快照中所见金额、期望写入的新金额。"""

    claim_id: int = Field(..., gt=0, description="理赔 id")
    observed_amount: int = Field(
        ..., ge=0, description="精算员在会话快照中所见的金额（非负整数，单位分）"
    )
    new_amount: int = Field(
        ..., ge=0, description="要更正为的新金额（非负整数，单位分）"
    )

    @model_validator(mode="after")
    def _amounts_must_differ(self) -> "ClaimCorrectionItem":
        if self.new_amount == self.observed_amount:
            raise ValueError("new_amount 必须与 observed_amount 不同")
        return self


class ClaimCorrectionRequest(BaseModel):
    """整批更正请求：有效会话 id + 至多 20 条更正指令。"""

    session_id: str = Field(..., min_length=1, max_length=64, description="快照会话 id")
    corrections: list[ClaimCorrectionItem] = Field(
        ...,
        min_length=1,
        max_length=CORRECTION_BATCH_LIMIT,
        description=f"更正条目，1..{CORRECTION_BATCH_LIMIT} 条，整批成功或整批拒绝",
    )

    @model_validator(mode="after")
    def _no_duplicate_claim_ids(self) -> "ClaimCorrectionRequest":
        seen: set[int] = set()
        duplicates: list[int] = []
        for item in self.corrections:
            if item.claim_id in seen and item.claim_id not in duplicates:
                duplicates.append(item.claim_id)
            seen.add(item.claim_id)
        if duplicates:
            raise ValueError(
                "corrections 中理赔 id 不得重复，重复 id："
                + ", ".join(str(i) for i in duplicates)
            )
        return self


class ClaimCorrectionApplied(BaseModel):
    """单条已落库的更正结果。"""

    claim_id: int
    old_amount: int = Field(..., description="快照所见、源端复核一致的原金额")
    new_amount: int


class ClaimCorrectionResult(BaseModel):
    """整批更正成功结果：要么全部写入，要么本响应不会出现。"""

    session_id: str
    region: str
    updated_count: int
    committed_at: datetime
    corrections: list[ClaimCorrectionApplied]


class Message(BaseModel):
    detail: str
    code: str | None = None
