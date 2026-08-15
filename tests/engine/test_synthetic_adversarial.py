"""M1.13c 合成图对抗用例（M1 册测试清单，+9，全 [pg]；每用例一行 GraphParams、种子写死）。

失败排障：SimRig 未静默时 AssertionError 自带 render_ascii 现场照与 seed，一键复现。
"""

from collections import Counter

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.types import NodeId
from tests.support.synthetic import (
    GraphParams,
    SimResult,
    SimRig,
    assert_invariants,
    expected_artifact_set,
)

pytestmark = pytest.mark.pg


@pytest.fixture
def rig(pg_engine: AsyncEngine) -> SimRig:
    return SimRig(pg_engine)


def _by_index(result: SimResult) -> list[str]:
    return [result.statuses[n.id] for n in result.graph.nodes]


async def test_smoke_small_graph_deterministic(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    params = GraphParams(n_nodes=20, seed=1301)
    first = await rig.run_to_quiescence(n_workers=3, params=params)
    second = await rig.run_to_quiescence(n_workers=3, params=params)
    # 同种子跑两遍：终态状态按生成序逐节点一致（03 §10.3"图终态一致"）
    assert _by_index(first) == _by_index(second)
    for result in (first, second):
        assert result.task_status == "DONE"
        await assert_invariants(
            pg_engine,
            result.task_id,
            result.execution_log,
            expected_artifacts=expected_artifact_set(result),
        )


async def test_wide_graph_scheduling(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3, params=GraphParams(n_nodes=20, depth=(3, 3), seed=1302)
    )
    # 宽图（单中间层 18 节点）打调度并发：全 DONE
    assert set(result.statuses.values()) == {"DONE"}
    assert result.task_status == "DONE"
    await assert_invariants(
        pg_engine,
        result.task_id,
        result.execution_log,
        expected_artifacts=expected_artifact_set(result),
    )


async def test_deep_graph_promotion_chain(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3, params=GraphParams(n_nodes=12, depth=(10, 10), seed=1303)
    )
    # 深图（10 层窄链）打促升链：全 DONE
    assert set(result.statuses.values()) == {"DONE"}
    assert result.task_status == "DONE"
    await assert_invariants(
        pg_engine,
        result.task_id,
        result.execution_log,
        expected_artifacts=expected_artifact_set(result),
    )


async def test_failure_injection_matrix(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3, params=GraphParams(n_nodes=30, fail_rate=0.3, seed=1304)
    )
    await assert_invariants(pg_engine, result.task_id, result.execution_log)
    rows = (
        await graph_session.execute(
            text("SELECT status, failure_class FROM plan_nodes WHERE task_id = :t"),
            {"t": result.task_id},
        )
    ).all()
    failed = [r for r in rows if r.status == "FAILED"]
    assert failed, "fail_rate=0.3 应产出 FAILED（永败/超时剧本）"
    for r in failed:
        assert r.failure_class == "retry_exhausted"  # FAILED 都带 failure_class
    signal_count = (
        await graph_session.execute(
            text("SELECT count(*) FROM replan_signals WHERE task_id = :t"),
            {"t": result.task_id},
        )
    ).scalar_one()
    # 信号行与 T5 数量吻合：每个 FAILED 恰一张 engine 自动生成的 node_replan 条子
    assert int(signal_count) == len(failed)


async def test_effectful_mix_step_resume(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3,
        params=GraphParams(
            n_nodes=30, effectful_ratio=0.5, crash_injections=2, slow_ratio=0.4, seed=1305
        ),
    )
    assert result.kills_done == 2
    await assert_invariants(
        pg_engine,
        result.task_id,
        result.execution_log,
        expected_artifacts=expected_artifact_set(result),
    )
    steps_rows = (
        await graph_session.execute(
            text(
                "SELECT node_id, count(*) FILTER (WHERE status = 'DONE') AS done_steps "
                "FROM node_steps GROUP BY node_id"
            )
        )
    ).all()
    done_steps = {NodeId(r.node_id): int(r.done_steps) for r in steps_rows}
    for node in result.graph.nodes:
        entry = result.script.get((node.id, 1))
        if entry is None or entry.action != "effectful_steps":
            continue
        if result.statuses[node.id] == "DONE":
            # 步骤零重做：每步恰一行 DONE（PK 保证唯一），总数与剧本步数吻合
            assert done_steps.get(node.id, 0) == entry.steps


async def test_crash_injection_zero_redo(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3,
        params=GraphParams(n_nodes=100, crash_injections=3, slow_ratio=0.2, seed=1306),
    )
    assert result.kills_done == 3
    # 密集崩溃可把同一节点连坑两次（attempt 耗尽 FAILED）——那是毒节点收敛的合法行为；
    # "零重做"的口径是工件侧（毕业验收第 2 行）：DONE 恰一件、早期 attempt 零残留，
    # 由 assert_invariants 的 I5/I7 断言 + expected_artifacts 集合比对承担
    assert "DONE" in set(result.statuses.values())
    await assert_invariants(
        pg_engine,
        result.task_id,
        result.execution_log,
        expected_artifacts=expected_artifact_set(result),
    )
    # 完成 attempt 的完整执行恰一次（同 attempt 零双执行由 I7 承担；此处对齐 DB 终局 attempt）
    done_attempt_runs = Counter(
        (rec.node_id, rec.attempt)
        for rec in result.execution_log
        if result.statuses.get(rec.node_id) == "DONE"
        and rec.attempt == result.attempts[rec.node_id]
    )
    redo = {key: n for key, n in done_attempt_runs.items() if n != 1}
    assert not redo, f"DONE 节点完成 attempt 的完整执行次数异常：{redo}"


async def test_cancel_injection_converges(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3,
        params=GraphParams(n_nodes=30, cancel_injections=3, slow_ratio=0.3, seed=1307),
    )
    assert result.cancels_done == 3
    await assert_invariants(pg_engine, result.task_id, result.execution_log)
    dangling = (
        await graph_session.execute(
            text(
                "SELECT count(*) FROM plan_nodes WHERE task_id = :t "
                "AND cancel_requested_at IS NOT NULL "
                "AND status NOT IN ('DONE','FAILED','CANCELLED')"
            ),
            {"t": result.task_id},
        )
    ).scalar_one()
    assert int(dangling) == 0  # 被取消子树全收敛：无悬空 RUNNING、无滞留意图
    assert "CANCELLED" in set(result.statuses.values())


async def test_cancel_crash_combo_branch3(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=3,
        params=GraphParams(
            n_nodes=20,
            slow_ratio=0.95,
            slow_seconds=(30.0, 60.0),
            cancel_crash_combo=2,
            seed=1308,
        ),
    )
    # 单根图上第一发 combo 可能收编全图（子树=全图），第二发无对象如实放弃——≥1 即达意
    assert result.combos_done >= 1
    await assert_invariants(pg_engine, result.task_id, result.execution_log)
    rows = (
        await graph_session.execute(
            text(
                "SELECT id, status FROM plan_nodes WHERE task_id = :t "
                "AND cancel_requested_at IS NOT NULL"
            ),
            {"t": result.task_id},
        )
    ).all()
    assert rows, "combo 应留下带取消意图的节点"
    for r in rows:
        # 取消中崩溃：reaper 分支③直接 CANCELLED，绝不回 READY（03 §10.1 表末行 / G.4③）
        assert r.status == "CANCELLED", f"combo 节点 {r.id} 终局 {r.status}"


async def test_full_100_node_mixed_adversarial(
    graph_session: AsyncSession, pg_engine: AsyncEngine, rig: SimRig
) -> None:
    result = await rig.run_to_quiescence(
        n_workers=4,
        params=GraphParams(
            n_nodes=100,
            depth=(3, 10),
            fanout=(2, 20),
            effectful_ratio=0.5,
            fail_rate=0.2,
            slow_ratio=0.1,
            crash_injections=3,
            cancel_injections=3,
            cancel_crash_combo=2,
            seed=1309,
        ),
    )
    assert result.kills_done == 3
    assert result.cancels_done == 3
    # 全维度混注终局：不变量全过 + 工件集合一致 + 执行日志零双执行（毕业验收第 1 行）
    await assert_invariants(
        pg_engine,
        result.task_id,
        result.execution_log,
        expected_artifacts=expected_artifact_set(result),
    )
