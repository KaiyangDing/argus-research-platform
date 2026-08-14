"""M1.1 类型化 DAG 模型与状态机合法集测试（M1 册测试清单，+8，纯单测）。"""

import uuid
from decimal import Decimal
from itertools import product
from typing import Any

import pytest
from pydantic import ValidationError

from argus.core.types import (
    ArtifactKind,
    ArtifactRef,
    ContractId,
    Granularity,
    NodeId,
    RoleName,
    TaskId,
    parse_contract_id,
)
from argus.engine.graph import (
    LEGAL_TRANSITIONS,
    BudgetRequest,
    IllegalTransition,
    NodeStatus,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
    assert_transition,
)


def _node(**overrides: Any) -> PlanNode:
    """最小合法 PlanNode；用例按需覆盖字段。"""
    base: dict[str, Any] = {
        "id": NodeId(uuid.uuid4()),
        "role": RoleName.RESEARCHER,
        "brief": TaskBrief(objective="查 2024 营收"),
        "inputs": (),
        "purity": Purity.PURE,
        "budget": BudgetRequest(tokens=1000, yuan=Decimal("0.5")),
    }
    base.update(overrides)
    return PlanNode(**base)


def test_purity_enum_values() -> None:
    # 03 §1.1 两值逐字
    assert [p.value for p in Purity] == ["pure", "effectful"]


def test_plan_node_frozen_and_extra_forbid() -> None:
    node = _node()
    with pytest.raises(ValidationError):
        node.priority = 5  # frozen：实例赋值必须抛
    with pytest.raises(ValidationError):
        _node(node_type="research")  # 未知字段：extra="forbid"（03 §1.1 字段集之外不收）


def test_plan_node_defaults() -> None:
    node = _node()
    # 与 02 DDL 列默认一致（速查表 B.1）
    assert node.priority == 0
    assert node.max_attempts == 2
    assert node.replan_on_failure is False


def test_contract_id_shape() -> None:
    assert parse_contract_id("research_memo@2") == ("research_memo", 2)
    for bad in ["Memo@2", "memo@0", "memo"]:
        with pytest.raises(ValueError):
            parse_contract_id(bad)


def test_plan_graph_json_roundtrip() -> None:
    a = _node()
    b = _node(
        inputs=(ArtifactRef(artifact_id=uuid.uuid4(), granularity=Granularity.DIGEST),),
        purity=Purity.EFFECTFUL,
    )
    graph = PlanGraph(
        task_id=TaskId(uuid.uuid4()),
        version=1,
        nodes=(a, b),
        edges=(PlanEdge(src=a.id, dst=b.id, contract=ContractId("research_memo@1")),),
    )
    restored = PlanGraph.model_validate_json(graph.model_dump_json())
    assert restored == graph


def test_legal_transitions_exactly_t1_t11() -> None:
    ns = NodeStatus
    expected = {
        (ns.PENDING, ns.READY),  # T1 前驱齐备促升
        (ns.READY, ns.RUNNING),  # T2 领取
        (ns.RUNNING, ns.READY),  # T3 可重试回队
        (ns.RUNNING, ns.DONE),  # T4 成功收尾
        (ns.RUNNING, ns.FAILED),  # T5 终态失败
        (ns.RUNNING, ns.NEEDS_REPLAN),  # T6 重规划滞留
        (ns.RUNNING, ns.CANCELLED),  # T7 取消收敛
        (ns.PENDING, ns.CANCELLED),  # T8 未开工即时取消
        (ns.READY, ns.CANCELLED),  # T8 未开工即时取消
        (ns.NEEDS_REPLAN, ns.PENDING),  # T9 手术后重置（M2 实现）
        (ns.NEEDS_REPLAN, ns.CANCELLED),  # T10 手术裁剪（M2 实现）
        (ns.READY, ns.PENDING),  # T11 手术降级（M2 实现，Z-14 以 03 为准）
    }
    # T8 两态展开后恰 12 个二元组，逐对核对速查表 C.2
    assert len(expected) == 12
    assert LEGAL_TRANSITIONS == frozenset(expected)


def test_assert_transition_illegal_raises() -> None:
    # 7×7 全组合循环（不用 parametrize，冻结区 2.5 #20）：合法通过、其余全抛
    # （05 §4.2 任务2"全迁移矩阵单测"）
    for current, new in product(NodeStatus, NodeStatus):
        if (current, new) in LEGAL_TRANSITIONS:
            assert_transition(current, new)  # 不抛即过
        else:
            with pytest.raises(IllegalTransition) as exc_info:
                assert_transition(current, new)
            assert exc_info.value.current is current
            assert exc_info.value.new is new


def test_role_and_artifact_kind_enum_values() -> None:
    # 速查表 D.1 #5/#6/#7 逐字，值冻结
    assert [r.value for r in RoleName] == [
        "planner",
        "researcher",
        "analyst",
        "verifier",
        "synthesizer",
    ]
    assert [k.value for k in ArtifactKind] == [
        "plan_snapshot",
        "research_note",
        "analysis_table",
        "verification_verdict",
        "report_draft",
        "report_final",
    ]
    assert [g.value for g in Granularity] == ["headline", "digest", "full"]
