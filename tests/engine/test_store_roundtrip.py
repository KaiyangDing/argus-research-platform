"""M1.4 PlanGraph 持久化往返测试（M1 册测试清单，+4，全 [pg]）。"""

import uuid
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from argus.core.types import ArtifactRef, ContractId, Granularity, NodeId, RoleName, TaskId
from argus.engine.graph import BudgetRequest, PlanEdge, PlanGraph, PlanNode, Purity, TaskBrief
from argus.engine.store import GraphInvalid, load_graph, persist_graph

pytestmark = pytest.mark.pg

_MK_TASK = text(
    """
    INSERT INTO research_tasks
        (id, title, objective, status, corpus_hash, budget_tokens_cap, budget_yuan_cap,
         requested_by)
    VALUES (:id, 't', 'o', 'EXECUTING', 'sha256:test', 100000, 100.00, 'tester')
    """
)


def _node(role: RoleName = RoleName.RESEARCHER, **overrides: Any) -> PlanNode:
    base: dict[str, Any] = {
        "id": NodeId(uuid.uuid4()),
        "role": role,
        "brief": TaskBrief(objective="查 2024 营收"),
        "inputs": (),
        "purity": Purity.PURE,
        "budget": BudgetRequest(tokens=100, yuan=Decimal("1.50")),
    }
    base.update(overrides)
    return PlanNode(**base)


def _diamond(task_id: TaskId) -> PlanGraph:
    """planner → 2×researcher → synthesizer 菱形；version=0 对齐 plan_version DDL 默认。"""
    a = _node(RoleName.PLANNER)
    b = _node()
    c = _node(purity=Purity.EFFECTFUL)
    d = _node(
        RoleName.SYNTHESIZER,
        inputs=(ArtifactRef(artifact_id=uuid.uuid4(), granularity=Granularity.FULL),),
        replan_on_failure=True,
    )
    return PlanGraph(
        task_id=task_id,
        version=0,
        nodes=(a, b, c, d),
        edges=(
            PlanEdge(src=a.id, dst=b.id, contract=ContractId("research_brief@1")),
            PlanEdge(src=a.id, dst=c.id, contract=ContractId("research_brief@1")),
            PlanEdge(src=b.id, dst=d.id, contract=ContractId("research_memo@1")),
            PlanEdge(src=c.id, dst=d.id, contract=ContractId("research_memo@1")),
        ),
    )


async def _persist(session: AsyncSession, graph: PlanGraph) -> None:
    await session.execute(_MK_TASK, {"id": graph.task_id})
    await session.commit()
    await persist_graph(session, graph, plan_version_added=0)


async def test_persist_graph_rows_and_spec_shape(graph_session: AsyncSession) -> None:
    graph = _diamond(TaskId(uuid.uuid4()))
    await _persist(graph_session, graph)
    counts = await graph_session.execute(
        text(
            "SELECT (SELECT count(*) FROM plan_nodes WHERE task_id = :t),"
            " (SELECT count(*) FROM plan_edges WHERE task_id = :t)"
        ),
        {"t": graph.task_id},
    )
    assert counts.one() == (4, 4)
    rows = await graph_session.execute(
        text("SELECT id, node_type, role, spec FROM plan_nodes WHERE task_id = :t"),
        {"t": graph.task_id},
    )
    by_id = {r.id: r for r in rows}
    planner = next(n for n in graph.nodes if n.role is RoleName.PLANNER)
    sink = next(n for n in graph.nodes if n.role is RoleName.SYNTHESIZER)
    for node in graph.nodes:
        row = by_id[node.id]
        # spec JSONB 五键形状（冻结区 2.5 #8）
        assert set(row.spec) == {
            "brief",
            "inputs",
            "output_contract",
            "budget",
            "replan_on_failure",
        }
        assert row.role == node.role.value
    assert by_id[planner.id].node_type == "plan"  # ROLE_TO_NODE_TYPE 派生
    assert by_id[sink.id].node_type == "synthesize"
    assert by_id[planner.id].spec["output_contract"] == "research_brief@1"  # 由出边派生
    assert by_id[sink.id].spec["output_contract"] is None  # sink 无出边 → None
    assert by_id[sink.id].spec["replan_on_failure"] is True


async def test_persist_promotes_sourceless_to_ready(graph_session: AsyncSession) -> None:
    graph = _diamond(TaskId(uuid.uuid4()))
    await _persist(graph_session, graph)
    rows = await graph_session.execute(
        text("SELECT id, status, ready_at FROM plan_nodes WHERE task_id = :t"),
        {"t": graph.task_id},
    )
    by_id = {r.id: r for r in rows}
    planner = next(n for n in graph.nodes if n.role is RoleName.PLANNER)
    for node in graph.nodes:
        if node.id == planner.id:  # 唯一无前驱节点：初始促升（03-T1 任务启动时刻等价物）
            assert by_id[node.id].status == "READY"
            assert by_id[node.id].ready_at is not None
        else:
            assert by_id[node.id].status == "PENDING"
            assert by_id[node.id].ready_at is None


async def test_load_graph_roundtrip_equals(graph_session: AsyncSession) -> None:
    graph = _diamond(TaskId(uuid.uuid4()))
    await _persist(graph_session, graph)
    loaded = await load_graph(graph_session, graph.task_id)
    assert loaded.task_id == graph.task_id
    assert loaded.version == graph.version
    # 集合等值：按 id 比对，不比顺序
    assert {n.id: n for n in loaded.nodes} == {n.id: n for n in graph.nodes}
    assert {(e.src, e.dst, e.contract) for e in loaded.edges} == {
        (e.src, e.dst, e.contract) for e in graph.edges
    }


async def test_persist_rejects_cyclic_graph(graph_session: AsyncSession) -> None:
    task_id = TaskId(uuid.uuid4())
    a, b = _node(), _node()
    cyclic = PlanGraph(
        task_id=task_id,
        version=0,
        nodes=(a, b),
        edges=(
            PlanEdge(src=a.id, dst=b.id, contract=ContractId("research_memo@1")),
            PlanEdge(src=b.id, dst=a.id, contract=ContractId("research_memo@1")),
        ),
    )
    await graph_session.execute(_MK_TASK, {"id": task_id})
    await graph_session.commit()
    with pytest.raises(GraphInvalid):
        await persist_graph(graph_session, cyclic, plan_version_added=0)
    count = await graph_session.execute(
        text("SELECT count(*) FROM plan_nodes WHERE task_id = :t"), {"t": task_id}
    )
    assert count.scalar_one() == 0  # 落库前校验拦截，库中零行
