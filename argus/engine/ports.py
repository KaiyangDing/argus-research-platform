"""执行端口：engine 与执行体之间的窄接口（03 §1.2；速查表 D.1 #33）。

engine 不知道执行体是什么（LLM/剧本/沙箱）——只认 NodeExecutor 协议与 NodeOutcome
三变体。ArtifactDraft 是不透明落库单元（M1 册冲突 C-2 临时口径：engine 不解析 payload）。
BudgetHooks 是 M2 预算记账的挂点，M1 以 NullBudgetHooks 空转（冻结区 2.5 #7）。
"""

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Final, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy.ext.asyncio import AsyncSession

from argus.core.types import ArtifactKind, ArtifactRef, ContractId, NodeId, RoleName, TaskId
from argus.engine.clock import Clock
from argus.engine.graph import FailureClass, NodeStatus, TaskBrief
from argus.engine.steps import StepJournal

if TYPE_CHECKING:
    from argus.engine.cancel import CancelToken


_KIND_DEFAULT_SEVERITY: Final[dict[str, int]] = {
    "node_replan": 2,
    "evidence_conflict": 2,
    "coverage_gap": 1,
    "verifier_negative": 3,
}

_CONTENT_HASH_RE: Final[re.Pattern[str]] = re.compile(r"^sha256:[0-9a-f]{64}$")


def _severity_for_kind(data: dict[str, Any]) -> int:
    """severity 缺省值：按 kind 查权重表（速查表 C.4：2/2/1/3）。"""
    return _KIND_DEFAULT_SEVERITY.get(data.get("kind", ""), 0)


class ReplanSignalDraft(BaseModel):
    """重规划信号草稿（03 §5.1 字段线索；severity 缺省按 kind 权重，速查表 C.4）。"""

    kind: Literal["node_replan", "evidence_conflict", "coverage_gap", "verifier_negative"]
    severity: int = Field(default_factory=_severity_for_kind)
    payload: dict[str, Any] = {}


class ArtifactDraft(BaseModel):
    """engine 不透明落库单元（冲突 C-2 临时口径）：终态事务内原样 INSERT，不解析。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: ArtifactKind
    schema_name: str
    schema_version: int = 1
    payload: dict[str, Any]
    headline: str
    digest: str | None = None
    refs: tuple[dict[str, Any], ...] = ()
    topic_keys: tuple[str, ...] = ()
    partial: bool = False
    token_count: int | None = None
    content_hash: str

    @field_validator("content_hash")
    @classmethod
    def _hash_shape(cls, v: str) -> str:
        if _CONTENT_HASH_RE.match(v) is None:
            raise ValueError(f"content_hash must match sha256:<64hex>, got {v!r}")
        return v

    @staticmethod
    def hash_payload(payload: dict[str, Any]) -> str:
        """canonical json → sha256（冻结区 2.5 #19）：键序无关、紧凑分隔、保留非 ASCII。"""
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class OutcomeSuccess(BaseModel):
    artifact: ArtifactDraft
    replan_signal: ReplanSignalDraft | None = None


class OutcomeDegraded(BaseModel):
    """预算耗尽的降级产出（03 §1.2/§6.4）：产物必须自我声明 partial。"""

    artifact: ArtifactDraft
    replan_signal: ReplanSignalDraft | None = None

    @field_validator("artifact")
    @classmethod
    def _must_be_partial(cls, v: ArtifactDraft) -> ArtifactDraft:
        if not v.partial:
            raise ValueError("degraded outcome requires artifact.partial=True")
        return v


class OutcomeFailure(BaseModel):
    failure_class: FailureClass | None = None  # 终态才必填；重试路径可 None
    error: dict[str, Any]
    retryable: bool
    replan_signal: ReplanSignalDraft | None = None


NodeOutcome = OutcomeSuccess | OutcomeDegraded | OutcomeFailure


class BudgetView(Protocol):
    """执行期预算视图（【实现细则】M1 占位；M2 实装"预留−已用"）。"""

    async def remaining_tokens(self) -> int | None: ...  # None = 无限（M1）


class NullBudgetView:
    """M1 无预算：余量恒 None（无限）。"""

    async def remaining_tokens(self) -> int | None:
        return None


@dataclass(frozen=True)
class NodeContext:
    """执行体拿到的全部世界（03 §1.2）：身份三元组 + 简报/输入/预算/取消/日志/时钟。"""

    task_id: TaskId
    node_id: NodeId
    attempt: int
    brief: TaskBrief
    inputs: tuple[ArtifactRef, ...]  # M1 不解析（bus 是 M3）
    budget: BudgetView
    cancel_token: CancelToken  # M1.12 收紧；py3.14 延迟注解下前向引用安全
    step_journal: StepJournal | None  # EFFECTFUL 才注入（M1.9 收紧；PURE 恒 None）
    clock: Clock


class NodeExecutor(Protocol):
    """窄接口（03 §1.2 逐字）：execute/resume 之外不加方法。"""

    async def execute(self, ctx: NodeContext) -> NodeOutcome: ...

    async def resume(self, ctx: NodeContext, from_step: int) -> NodeOutcome: ...


class ContractPair(BaseModel):
    """角色的输入/输出契约对（【实现细则】：03 §1.2 点名类型未给字段）。"""

    input_contract: ContractId | None = None
    output_contract: ContractId | None = None


class BudgetHint(BaseModel):
    """角色级成本先验（【实现细则】；03 §6.2 冷启动注入用，M2 消费）。"""

    est_tokens: int
    est_yuan: Decimal


class ExecutorRegistry(Protocol):
    """role → 执行体解析（03 §1.2 逐字）；生产注册表 M3 由 roster 提供。"""

    def resolve(self, role: RoleName) -> tuple[NodeExecutor, ContractPair, BudgetHint]: ...


class BudgetHooks(Protocol):
    """M2 预算记账挂点（冻结区 2.5 #7）：全部在调用方事务的同一连接上执行。"""

    async def on_claim(
        self, conn: AsyncSession, *, task_id: TaskId, node_id: NodeId, attempt: int
    ) -> None: ...

    async def on_terminal(
        self,
        conn: AsyncSession,
        *,
        task_id: TaskId,
        node_id: NodeId,
        attempt: int,
        terminal: NodeStatus,
    ) -> None: ...

    async def on_prune(self, conn: AsyncSession, *, node_ids: Sequence[NodeId]) -> None: ...


class NullBudgetHooks:
    """M1 空实现：挂点在、动作无（预算记账 M2 的 budget.py 注入真实现）。"""

    async def on_claim(
        self, conn: AsyncSession, *, task_id: TaskId, node_id: NodeId, attempt: int
    ) -> None:
        return None

    async def on_terminal(
        self,
        conn: AsyncSession,
        *,
        task_id: TaskId,
        node_id: NodeId,
        attempt: int,
        terminal: NodeStatus,
    ) -> None:
        return None

    async def on_prune(self, conn: AsyncSession, *, node_ids: Sequence[NodeId]) -> None:
        return None
