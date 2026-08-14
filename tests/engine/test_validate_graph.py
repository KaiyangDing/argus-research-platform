"""M1.4 validate_graph 不变量校验测试（M1 册测试清单，+6，纯单测）。"""

import uuid
from decimal import Decimal
from typing import Any

from argus.core.types import ContractId, NodeId, RoleName, TaskId
from argus.engine.graph import (
    BudgetRequest,
    NodeStatus,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
)
from argus.engine.validate import ViolationCode, validate_graph


def _node(role: RoleName = RoleName.RESEARCHER, **overrides: Any) -> PlanNode:
    base: dict[str, Any] = {
        "id": NodeId(uuid.uuid4()),
        "role": role,
        "brief": TaskBrief(objective="o"),
        "inputs": (),
        "purity": Purity.PURE,
        "budget": BudgetRequest(tokens=100, yuan=Decimal("1.0")),
    }
    base.update(overrides)
    return PlanNode(**base)


def _edge(src: PlanNode, dst: PlanNode, contract: str = "research_memo@1") -> PlanEdge:
    return PlanEdge(src=src.id, dst=dst.id, contract=ContractId(contract))


def _graph(nodes: tuple[PlanNode, ...], edges: tuple[PlanEdge, ...]) -> PlanGraph:
    return PlanGraph(task_id=TaskId(uuid.uuid4()), version=1, nodes=nodes, edges=edges)


def _codes(graph: PlanGraph, **kwargs: Any) -> set[ViolationCode]:
    return {v.code for v in validate_graph(graph, **kwargs).violations}


def test_i1_cycle_detected() -> None:
    a, b, c = _node(), _node(), _node()
    report = validate_graph(_graph((a, b, c), (_edge(a, b), _edge(b, c), _edge(c, a))))
    cycles = [v for v in report.violations if v.code is ViolationCode.CYCLE]
    assert len(cycles) == 1
    assert set(cycles[0].node_ids) == {a.id, b.id, c.id}  # Kahn 剩余节点恰为环成员


def test_i2_orphan_and_dangling() -> None:
    a, b = _node(), _node()
    ghost = NodeId(uuid.uuid4())
    # b 的唯一入边来自不存在的节点：边悬空 + b 从根不可达，两码同报
    ghost_edge = PlanEdge(src=ghost, dst=b.id, contract=ContractId("research_memo@1"))
    codes = _codes(_graph((a, b), (ghost_edge,)))
    assert ViolationCode.DANGLING_EDGE in codes
    assert ViolationCode.ORPHAN in codes


def test_i2_failed_predecessor_is_warning() -> None:
    a, b = _node(), _node()
    graph = _graph((a, b), (_edge(a, b),))
    report = validate_graph(graph, statuses={a.id: NodeStatus.FAILED, b.id: NodeStatus.PENDING})
    # 03 §2.4 注²：前驱 FAILED 未处置是修复中间态——警告不是违规
    assert report.violations == ()
    assert len(report.warnings) == 1
    assert report.warnings[0].node_ids == (b.id,)


def test_i3_budget_over_cap() -> None:
    a = _node(budget=BudgetRequest(tokens=600, yuan=Decimal("3.0")))
    b = _node(budget=BudgetRequest(tokens=500, yuan=Decimal("2.0")))
    graph = _graph((a, b), (_edge(a, b),))
    over = _codes(graph, task_caps=BudgetRequest(tokens=1000, yuan=Decimal("10.0")))
    assert ViolationCode.BUDGET_OVER_CAP in over  # Σtokens=1100 > 1000
    within = _codes(graph, task_caps=BudgetRequest(tokens=2000, yuan=Decimal("10.0")))
    assert ViolationCode.BUDGET_OVER_CAP not in within
    skipped = _codes(graph)  # task_caps=None → 跳过 I3
    assert ViolationCode.BUDGET_OVER_CAP not in skipped


def test_contract_shape_and_mixed_out_edges() -> None:
    a, b, c = _node(), _node(), _node()
    # model_construct 绕过 pydantic 校验器——模拟"从库里读回坏数据"的纵深防御场景
    bad = PlanEdge.model_construct(src=a.id, dst=b.id, contract="Bad@0")
    codes = _codes(_graph((a, b), (bad,)))
    assert ViolationCode.CONTRACT_SHAPE in codes
    mixed = _codes(
        _graph((a, b, c), (_edge(a, b, "research_memo@1"), _edge(a, c, "analysis_table@1")))
    )
    # 同 src 两出边契约不一致：一个节点只有一种输出契约（冻结区 2.5 #8 派生约束）
    assert ViolationCode.OUT_EDGE_CONTRACT_MIXED in mixed


def test_valid_graph_ok() -> None:
    a = _node(RoleName.PLANNER)
    b, c = _node(), _node()
    d = _node(RoleName.SYNTHESIZER)
    report = validate_graph(
        _graph(
            (a, b, c, d),
            (
                _edge(a, b, "research_brief@1"),
                _edge(a, c, "research_brief@1"),
                _edge(b, d),
                _edge(c, d),
            ),
        ),
        task_caps=BudgetRequest(tokens=10000, yuan=Decimal("100.0")),
    )
    assert report.ok
    assert report.warnings == ()
