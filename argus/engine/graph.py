"""任务图静态模型与节点/任务状态机（03 §1.1/§2.2；速查表 C.1/C.2/D.1）。

模型是法律：字段一个不加一个不减（03 §1.1）；DB 列映射是 store 的事（M1.4）。
迁移合法集 T1-T11 一次录全，实现按里程碑分期（M1 做 T1-T8，图手术三条 M2 接入）。
"""

from decimal import Decimal
from enum import StrEnum
from typing import Final

from pydantic import BaseModel, ConfigDict, Field, field_validator

from argus.core.types import (
    CONTRACT_ID_RE,
    ArgusError,
    ArtifactRef,
    ContractId,
    NodeId,
    RoleName,
    TaskId,
)


class Purity(StrEnum):
    """节点纯度（03 §1.1）：PURE 可整节点安全重跑；EFFECTFUL 有外部副作用，须步骤日志续跑。"""

    PURE = "pure"
    EFFECTFUL = "effectful"


class NodeStatus(StrEnum):  # 速查表 D.1 #2，七值
    PENDING = "PENDING"
    READY = "READY"
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    NEEDS_REPLAN = "NEEDS_REPLAN"


class TaskStatus(StrEnum):  # 速查表 D.1 #1，八值
    SUBMITTED = "SUBMITTED"
    PLANNING = "PLANNING"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    EXECUTING = "EXECUTING"
    DONE = "DONE"
    DONE_DEGRADED = "DONE_DEGRADED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class FailureClass(StrEnum):  # 六值本册冻结（M1 册冻结区 2.5 #4；03 §9 散见值并集）
    CONTRACT_VIOLATION = "contract_violation"
    BUDGET_EXHAUSTED = "budget_exhausted"
    CANCEL_TIMEOUT = "cancel_timeout"
    RETRY_EXHAUSTED = "retry_exhausted"
    LLM_ERROR = "llm_error"
    TOOL_ERROR = "tool_error"


class TaskBrief(BaseModel):
    """任务/节点简报；engine 透传不解析（03 §1.1）。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    objective: str
    constraints: tuple[str, ...] = ()
    output_requirements: str = ""


class BudgetRequest(BaseModel):
    """节点预算申请：tokens 与 ¥ 上限（03 §1.1；记账 M2 接入，本步只定形状）。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    tokens: int = Field(ge=0)
    yuan: Decimal = Field(ge=0)


class PlanNode(BaseModel):
    """计划节点（03 §1.1 逐字段，一个不加一个不减；status/node_type 是 DB 侧概念，不进模型）。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    id: NodeId
    role: RoleName
    brief: TaskBrief
    inputs: tuple[ArtifactRef, ...]
    purity: Purity
    budget: BudgetRequest
    priority: int = 0
    max_attempts: int = 2
    replan_on_failure: bool = False


class PlanEdge(BaseModel):
    """依赖边（03 §1.1）：src 的输出按 contract 交给 dst；无环性由 validate_graph 守（M1.4）。"""

    src: NodeId
    dst: NodeId
    contract: ContractId

    @field_validator("contract")
    @classmethod
    def _contract_shape(cls, v: str) -> str:
        # 只校形状；注册表校验 M3 接入
        if CONTRACT_ID_RE.match(v) is None:
            raise ValueError(f"illegal contract id shape: {v!r}")
        return v


class PlanGraph(BaseModel):
    """整图快照（03 §1.1）：version 对应 research_tasks.plan_version（图手术每次 +1，M2）。"""

    task_id: TaskId
    version: int
    nodes: tuple[PlanNode, ...]
    edges: tuple[PlanEdge, ...]


# role → plan_nodes.node_type 固定映射（M1 册冻结区 2.5 #8；store 落库时派生，M1.4 用）
ROLE_TO_NODE_TYPE: Final[dict[RoleName, str]] = {
    RoleName.PLANNER: "plan",
    RoleName.RESEARCHER: "research",
    RoleName.ANALYST: "analyze",
    RoleName.VERIFIER: "verify",
    RoleName.SYNTHESIZER: "synthesize",
}

# T1-T11 迁移合法集（速查表 C.2；T8 覆盖 PENDING/READY 两态，展开后恰 12 个二元组）
LEGAL_TRANSITIONS: Final[frozenset[tuple[NodeStatus, NodeStatus]]] = frozenset(
    {
        (NodeStatus.PENDING, NodeStatus.READY),  # T1 前驱齐备促升
        (NodeStatus.READY, NodeStatus.RUNNING),  # T2 领取
        (NodeStatus.RUNNING, NodeStatus.READY),  # T3 可重试失败/租约过期回队
        (NodeStatus.RUNNING, NodeStatus.DONE),  # T4 成功收尾
        (NodeStatus.RUNNING, NodeStatus.FAILED),  # T5 终态失败
        (NodeStatus.RUNNING, NodeStatus.NEEDS_REPLAN),  # T6 重规划滞留
        (NodeStatus.RUNNING, NodeStatus.CANCELLED),  # T7 取消收敛
        (NodeStatus.PENDING, NodeStatus.CANCELLED),  # T8 未开工即时取消
        (NodeStatus.READY, NodeStatus.CANCELLED),  # T8 未开工即时取消
        (NodeStatus.NEEDS_REPLAN, NodeStatus.PENDING),  # T9 图手术后重置（M2 实现）
        (NodeStatus.NEEDS_REPLAN, NodeStatus.CANCELLED),  # T10 手术裁剪（M2 实现）
        (NodeStatus.READY, NodeStatus.PENDING),  # T11 手术降级（M2 实现；Z-14 以 03 为准）
    }
)


class IllegalTransition(ArgusError):
    """非法节点状态迁移（不在 T1-T11 合法集内）。"""

    def __init__(self, current: NodeStatus, new: NodeStatus) -> None:
        super().__init__(f"illegal node transition: {current} -> {new}")
        self.current = current
        self.new = new


def assert_transition(current: NodeStatus, new: NodeStatus) -> None:
    """不在 LEGAL_TRANSITIONS 内则抛 IllegalTransition；所有守卫 UPDATE 前先调用。"""
    if (current, new) not in LEGAL_TRANSITIONS:
        raise IllegalTransition(current, new)
