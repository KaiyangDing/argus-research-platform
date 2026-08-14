"""PlanGraph ⇄ plan_nodes/plan_edges 持久化与载入（M1 册【实现细则】新文件）。

engine 的图落库入口：写前必过 validate_graph；status 不写、吃 DDL 默认 'PENDING'；
无前驱节点同事务促升 READY（03-T1 在任务启动时刻的等价物）。图落库后只有状态机推动者写它。
"""

import json
from datetime import datetime

from pydantic import BaseModel, ConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from argus.core.types import ArgusError, ArtifactRef, ContractId, NodeId, RoleName, TaskId
from argus.engine.graph import (
    ROLE_TO_NODE_TYPE,
    BudgetRequest,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
)
from argus.engine.validate import ValidationReport, validate_graph


class NodeSpec(BaseModel):
    """plan_nodes.spec JSONB 的形状（M1 册冻结区 2.5 #8）。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    brief: TaskBrief
    inputs: tuple[ArtifactRef, ...] = ()
    output_contract: ContractId | None = None  # 由出边契约派生；sink 节点为 None
    budget: BudgetRequest
    replan_on_failure: bool = False


class NodeRuntimeRow(BaseModel):
    """RUNNING 执行期需要的行视图（【实现细则】；worker M1.10 消费）。"""

    model_config = ConfigDict(frozen=True)

    task_id: TaskId
    attempt: int
    max_attempts: int
    purity: Purity
    spec: NodeSpec
    cancel_requested_at: datetime | None


class GraphInvalid(ArgusError):
    """validate_graph 违规时拒绝落库；report 携带全部违规明细。"""

    def __init__(self, report: ValidationReport) -> None:
        super().__init__(f"graph invalid: {[str(v.code) for v in report.violations]}")
        self.report = report


_INSERT_NODE = text(
    """
    INSERT INTO plan_nodes
        (id, task_id, plan_version_added, node_type, role, spec, purity, priority, max_attempts)
    VALUES (:id, :task_id, :pva, :node_type, :role, CAST(:spec AS jsonb), :purity,
            :priority, :max_attempts)
    """
)

_INSERT_EDGE = text(
    """
    INSERT INTO plan_edges (task_id, from_node, to_node, plan_version_added)
    VALUES (:task_id, :src, :dst, :pva)
    """
)

_PROMOTE_SOURCELESS = text(
    """
    UPDATE plan_nodes SET status = 'READY', ready_at = now()
    WHERE task_id = :task_id AND status = 'PENDING'
      AND NOT EXISTS (SELECT 1 FROM plan_edges e WHERE e.to_node = plan_nodes.id)
    """
)


async def persist_graph(
    session: AsyncSession, graph: PlanGraph, *, plan_version_added: int
) -> None:
    """单事务写入整图；violations 非空抛 GraphInvalid（落库前拦截，库中零行）。"""
    report = validate_graph(graph)
    if not report.ok:
        raise GraphInvalid(report)
    out_contract: dict[NodeId, ContractId] = {}
    for edge in graph.edges:
        out_contract.setdefault(edge.src, edge.contract)  # validate 已保证同 src 不混
    node_rows = []
    for n in graph.nodes:
        spec = NodeSpec(
            brief=n.brief,
            inputs=n.inputs,
            output_contract=out_contract.get(n.id),
            budget=n.budget,
            replan_on_failure=n.replan_on_failure,
        )
        node_rows.append(
            {
                "id": n.id,
                "task_id": graph.task_id,
                "pva": plan_version_added,
                "node_type": ROLE_TO_NODE_TYPE[n.role],
                "role": n.role.value,
                "spec": json.dumps(spec.model_dump(mode="json"), ensure_ascii=False),
                "purity": n.purity.value,
                "priority": n.priority,
                "max_attempts": n.max_attempts,
            }
        )
    await session.execute(_INSERT_NODE, node_rows)
    if graph.edges:
        await session.execute(
            _INSERT_EDGE,
            [
                {"task_id": graph.task_id, "src": e.src, "dst": e.dst, "pva": plan_version_added}
                for e in graph.edges
            ],
        )
    await session.execute(_PROMOTE_SOURCELESS, {"task_id": graph.task_id})
    await session.commit()


async def load_graph(session: AsyncSession, task_id: TaskId) -> PlanGraph:
    """回读 PlanGraph（version = research_tasks.plan_version）；spec → PlanNode 还原。"""
    version_res = await session.execute(
        text("SELECT plan_version FROM research_tasks WHERE id = :t"), {"t": task_id}
    )
    version = version_res.scalar_one()
    node_res = await session.execute(
        text(
            "SELECT id, role, purity, priority, max_attempts, spec "
            "FROM plan_nodes WHERE task_id = :t"
        ),
        {"t": task_id},
    )
    nodes: list[PlanNode] = []
    out_contract: dict[NodeId, ContractId | None] = {}
    for row in node_res:
        spec = NodeSpec.model_validate(row.spec)
        node_id = NodeId(row.id)
        out_contract[node_id] = spec.output_contract
        nodes.append(
            PlanNode(
                id=node_id,
                role=RoleName(row.role),
                brief=spec.brief,
                inputs=spec.inputs,
                purity=Purity(row.purity),
                budget=spec.budget,
                priority=row.priority,
                max_attempts=row.max_attempts,
                replan_on_failure=spec.replan_on_failure,
            )
        )
    edge_res = await session.execute(
        text("SELECT from_node, to_node FROM plan_edges WHERE task_id = :t"), {"t": task_id}
    )
    edges: list[PlanEdge] = []
    for row in edge_res:
        contract = out_contract[NodeId(row.from_node)]
        if contract is None:  # persist 口径下不可能，防库中脏数据
            raise ArgusError(f"edge from {row.from_node} but src spec has no output_contract")
        edges.append(
            PlanEdge(src=NodeId(row.from_node), dst=NodeId(row.to_node), contract=contract)
        )
    return PlanGraph(task_id=task_id, version=version, nodes=tuple(nodes), edges=tuple(edges))


async def load_node_runtime(session: AsyncSession, node_id: NodeId) -> NodeRuntimeRow:
    """RUNNING 执行期行视图（worker 构造 NodeContext 用，M1.10）。"""
    res = await session.execute(
        text(
            "SELECT task_id, attempt, max_attempts, purity, spec, cancel_requested_at "
            "FROM plan_nodes WHERE id = :n"
        ),
        {"n": node_id},
    )
    row = res.one()
    return NodeRuntimeRow(
        task_id=TaskId(row.task_id),
        attempt=row.attempt,
        max_attempts=row.max_attempts,
        purity=Purity(row.purity),
        spec=NodeSpec.model_validate(row.spec),
        cancel_requested_at=row.cancel_requested_at,
    )
