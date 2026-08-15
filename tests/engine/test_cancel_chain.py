"""M1.12 取消传播链测试（M1 册测试清单，+8，全 [pg]）。"""

import asyncio
import json
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker
from argus.core.types import ContractId, NodeId, RoleName, TaskId
from argus.engine.cancel import request_cancel, subtree_ids
from argus.engine.graph import (
    BudgetRequest,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
)
from argus.engine.ports import NullBudgetHooks
from argus.engine.reaper import reap_once
from argus.engine.store import NodeSpec, persist_graph
from argus.engine.worker import EngineTuning, Worker
from tests.support.fake_clock import FakeClock
from tests.support.scripted_executor import ScriptedExecutor, ScriptEntry, ScriptTable
from tests.support.scripted_registry import ScriptedRegistry

pytestmark = pytest.mark.pg

_MK_TASK = text(
    """
    INSERT INTO research_tasks
        (id, title, objective, status, corpus_hash, budget_tokens_cap, budget_yuan_cap,
         requested_by)
    VALUES (:id, 't', 'o', 'EXECUTING', 'sha256:test', 100000, 100.00, 'tester')
    """
)

_MK_NODE = text(
    """
    INSERT INTO plan_nodes
        (id, task_id, plan_version_added, node_type, role, spec, status, attempt,
         lease_owner, lease_expires_at)
    VALUES (:id, :task, 0, 'research', 'researcher', CAST(:spec AS jsonb), :status, :attempt,
            :owner, now() + CAST(:ttl AS interval))
    """
)

_MK_EDGE = text(
    """
    INSERT INTO plan_edges (task_id, from_node, to_node, plan_version_added)
    VALUES (:task, :src, :dst, 0)
    """
)

_SPEC_JSON = json.dumps(
    NodeSpec(
        brief=TaskBrief(objective="o"),
        budget=BudgetRequest(tokens=100, yuan=Decimal("1.0")),
    ).model_dump(mode="json"),
    ensure_ascii=False,
)


async def _seed_task(session: AsyncSession) -> TaskId:
    task_id = TaskId(uuid.uuid4())
    await session.execute(_MK_TASK, {"id": task_id})
    await session.commit()
    return task_id


async def _seed_node(
    session: AsyncSession,
    task_id: TaskId,
    *,
    status: str = "RUNNING",
    attempt: int = 1,
    owner: str | None = "w1",
    ttl: timedelta | None = timedelta(seconds=90),
) -> NodeId:
    node_id = NodeId(uuid.uuid4())
    await session.execute(
        _MK_NODE,
        {
            "id": node_id,
            "task": task_id,
            "spec": _SPEC_JSON,
            "status": status,
            "attempt": attempt,
            "owner": owner,
            "ttl": ttl,
        },
    )
    await session.commit()
    return node_id


def _node(role: RoleName = RoleName.RESEARCHER, purity: Purity = Purity.PURE) -> PlanNode:
    return PlanNode(
        id=NodeId(uuid.uuid4()),
        role=role,
        brief=TaskBrief(objective="o"),
        inputs=(),
        purity=purity,
        budget=BudgetRequest(tokens=100, yuan=Decimal("1.0")),
    )


async def _persist(session: AsyncSession, graph: PlanGraph) -> None:
    await session.execute(_MK_TASK, {"id": graph.task_id})
    await session.commit()
    await persist_graph(session, graph, plan_version_added=0)


def _worker(
    pg_engine: AsyncEngine,
    clock: FakeClock,
    script: ScriptTable,
    *,
    tuning: EngineTuning | None = None,
    concurrency: int = 8,
) -> Worker:
    return Worker(
        session_factory=build_sessionmaker(pg_engine),
        registry=ScriptedRegistry(ScriptedExecutor(script, seed=7)),
        clock=clock,
        rng=random.Random(1),
        tuning=tuning if tuning is not None else EngineTuning(),
        concurrency=concurrency,
        hooks=NullBudgetHooks(),
    )


@asynccontextmanager
async def _running(worker: Worker) -> AsyncIterator[None]:
    task = asyncio.create_task(worker.run_forever())
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def _drive_until(
    clock: FakeClock,
    predicate: Callable[[], Awaitable[bool]],
    *,
    step: float = 1.0,
    max_iters: int = 400,
) -> None:
    for _ in range(max_iters):
        if await predicate():
            return
        clock.advance(step)
        await asyncio.sleep(0.02)
    raise AssertionError(f"did not converge within {max_iters} iters")


async def _node_row(session: AsyncSession, node_id: NodeId) -> Any:
    return (
        await session.execute(
            text(
                "SELECT status, lease_owner, lease_expires_at, cancel_requested_at, "
                "cancel_reason, failure_class, error, finished_at "
                "FROM plan_nodes WHERE id = :id"
            ),
            {"id": node_id},
        )
    ).one()


async def test_intent_and_immediate_t8_same_tx(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    pending = await _seed_node(
        graph_session, task, status="PENDING", attempt=0, owner=None, ttl=None
    )
    ready = await _seed_node(graph_session, task, status="READY", attempt=0, owner=None, ttl=None)
    running = await _seed_node(graph_session, task)
    await request_cancel(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_ids=[pending, ready, running],
        reason="user_abort",
        hooks=NullBudgetHooks(),
    )
    for node in (pending, ready):
        row = await _node_row(graph_session, node)
        # 无在途工作：同事务即时 T8 终态
        assert row.status == "CANCELLED"
        assert row.finished_at is not None
        assert row.cancel_requested_at is not None
        assert row.cancel_reason == "user_abort"
    run_row = await _node_row(graph_session, running)
    # RUNNING 只落意图，仍是 RUNNING（03 §2.1"取消意图不是状态"）；②③由 worker/reaper 收敛
    assert run_row.status == "RUNNING"
    assert run_row.cancel_requested_at is not None
    assert run_row.finished_at is None


async def test_subtree_cte_descendants_only(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    root = await _seed_node(graph_session, task, status="PENDING", owner=None, ttl=None)
    x = await _seed_node(graph_session, task, status="PENDING", owner=None, ttl=None)
    w = await _seed_node(graph_session, task, status="PENDING", owner=None, ttl=None)
    y = await _seed_node(graph_session, task, status="PENDING", owner=None, ttl=None)
    sibling = await _seed_node(graph_session, task, status="PENDING", owner=None, ttl=None)
    for src, dst in [(root, x), (root, w), (x, y), (w, y)]:  # 菱形 + 旁支 sibling
        await graph_session.execute(_MK_EDGE, {"task": task, "src": src, "dst": dst})
    await graph_session.commit()
    got = set(await subtree_ids(graph_session, root))
    assert got == {root, x, w, y}  # 含 root 自身与全部后代；不含兄弟
    assert sibling not in got
    assert set(await subtree_ids(graph_session, x)) == {x, y}


async def test_running_pure_converges_via_heartbeat(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    n = _node()
    task = TaskId(uuid.uuid4())
    await _persist(graph_session, PlanGraph(task_id=task, version=0, nodes=(n,), edges=()))
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="hang", hang_seconds=100_000),
    }
    worker = _worker(pg_engine, clock, script)
    factory = build_sessionmaker(pg_engine)

    async def _is_running() -> bool:
        return bool((await _node_row(graph_session, n.id)).status == "RUNNING")

    async def _is_cancelled() -> bool:
        return bool((await _node_row(graph_session, n.id)).status == "CANCELLED")

    async with _running(worker):
        await _drive_until(clock, _is_running)
        await request_cancel(
            factory, task_id=task, node_ids=[n.id], reason="op", hooks=NullBudgetHooks()
        )
        # 心跳（30s 虚拟）观察意图 → token → PURE 硬杀 → commit_cancelled
        await _drive_until(clock, _is_cancelled)
    row = await _node_row(graph_session, n.id)
    assert row.lease_owner is None
    assert row.lease_expires_at is None
    assert row.finished_at is not None
    task_status = (
        await graph_session.execute(
            text("SELECT status FROM research_tasks WHERE id = :t"), {"t": task}
        )
    ).scalar_one()
    assert task_status == "CANCELLED"  # 全终态 + 存在取消意图：分支 B


async def test_effectful_waits_step_boundary(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    n = _node(purity=Purity.EFFECTFUL)
    task = TaskId(uuid.uuid4())
    await _persist(graph_session, PlanGraph(task_id=task, version=0, nodes=(n,), edges=()))
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="effectful_steps", steps=3, step_seconds=50),
    }
    # 宽限给足：本用例验证的是步骤边界自行返回，不是宽限超时
    worker = _worker(pg_engine, clock, script, tuning=EngineTuning(t_grace_seconds=10_000))
    factory = build_sessionmaker(pg_engine)

    async def _step2_begun() -> bool:
        count = (
            await graph_session.execute(
                text("SELECT count(*) FROM node_steps WHERE node_id = :n AND step_no = 2"),
                {"n": n.id},
            )
        ).scalar_one()
        return int(count) == 1

    async def _is_cancelled() -> bool:
        return bool((await _node_row(graph_session, n.id)).status == "CANCELLED")

    async with _running(worker):
        await _drive_until(clock, _step2_begun, max_iters=100)
        await request_cancel(
            factory, task_id=task, node_ids=[n.id], reason="op", hooks=NullBudgetHooks()
        )
        await _drive_until(clock, _is_cancelled, max_iters=200)
    steps = (
        await graph_session.execute(
            text("SELECT step_no, status FROM node_steps WHERE node_id = :n ORDER BY step_no"),
            {"n": n.id},
        )
    ).all()
    # 第 2 步完成标记在、第 3 步未执行：执行体在步骤边界响应取消（03 §7.2 表）
    assert [(r.step_no, r.status) for r in steps] == [(1, "DONE"), (2, "DONE")]


async def test_grace_timeout_marks_cancel_timeout(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    n = _node(purity=Purity.EFFECTFUL)
    task = TaskId(uuid.uuid4())
    await _persist(graph_session, PlanGraph(task_id=task, version=0, nodes=(n,), edges=()))
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="hang", hang_seconds=1_000_000),  # 无步骤边界可让
    }
    worker = _worker(pg_engine, clock, script, tuning=EngineTuning(t_grace_seconds=10))
    factory = build_sessionmaker(pg_engine)

    async def _is_running() -> bool:
        return bool((await _node_row(graph_session, n.id)).status == "RUNNING")

    async def _is_failed() -> bool:
        return bool((await _node_row(graph_session, n.id)).status == "FAILED")

    async with _running(worker):
        await _drive_until(clock, _is_running)
        await request_cancel(
            factory, task_id=task, node_ids=[n.id], reason="op", hooks=NullBudgetHooks()
        )
        await _drive_until(clock, _is_failed)
    row = await _node_row(graph_session, n.id)
    assert row.failure_class == "cancel_timeout"  # T_grace 耗尽：硬杀 + 记败（租约过期兜底）
    assert row.error == {"cause": "cancel_grace_exceeded"}


async def test_cancel_idempotent_double_request(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)
    factory = build_sessionmaker(pg_engine)
    await request_cancel(
        factory, task_id=task, node_ids=[node], reason="first", hooks=NullBudgetHooks()
    )
    first = await _node_row(graph_session, node)
    await request_cancel(
        factory, task_id=task, node_ids=[node], reason="second", hooks=NullBudgetHooks()
    )
    second = await _node_row(graph_session, node)
    # 幂等：已有 cancel_requested_at 的行不覆盖（02 §2.3）——时刻与理由都保持第一次的
    assert second.cancel_requested_at == first.cancel_requested_at
    assert second.cancel_reason == "first"
    assert second.status == "RUNNING"


async def test_reaper_branch3_cancelled_not_ready(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, attempt=1, owner="w1")
    factory = build_sessionmaker(pg_engine)
    await request_cancel(
        factory, task_id=task, node_ids=[node], reason="op", hooks=NullBudgetHooks()
    )
    # 持有 worker"崩"：无人收敛，租约置过期（连接早已不存在，冻结区 2.5 #21 制造法）
    await graph_session.execute(
        text("UPDATE plan_nodes SET lease_expires_at = now() - interval '1 second' WHERE id = :id"),
        {"id": node},
    )
    await graph_session.commit()
    stats = await reap_once(factory, hooks=NullBudgetHooks())
    assert stats.terminal_cancelled == 1
    assert stats.requeued == 0  # 绝不回 READY（G.4③）
    row = await _node_row(graph_session, node)
    assert row.status == "CANCELLED"
    assert row.finished_at is not None
    assert row.lease_owner is None
    task_status = (
        await graph_session.execute(
            text("SELECT status FROM research_tasks WHERE id = :t"), {"t": task}
        )
    ).scalar_one()
    assert task_status == "CANCELLED"


async def test_cancel_storm_no_dangling_running(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    rng = random.Random(42)  # 种子固定：失败可一键复现（03 §10.2）
    nodes = [_node(RoleName.PLANNER)] + [_node() for _ in range(19)]
    edges: list[PlanEdge] = []
    for i in range(1, 20):
        for parent in rng.sample(range(i), k=min(i, rng.choice([1, 2]))):
            edges.append(
                PlanEdge(
                    src=nodes[parent].id,
                    dst=nodes[i].id,
                    contract=ContractId("research_memo@1"),
                )
            )
    task = TaskId(uuid.uuid4())
    await _persist(
        graph_session,
        PlanGraph(task_id=task, version=0, nodes=tuple(nodes), edges=tuple(edges)),
    )
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="hang", hang_seconds=40) for n in nodes
    }
    worker = _worker(pg_engine, clock, script)
    factory = build_sessionmaker(pg_engine)

    async def _converged() -> bool:
        counts = (
            await graph_session.execute(
                text(
                    "SELECT"
                    " (SELECT count(*) FROM plan_nodes WHERE task_id = :t"
                    "   AND status = 'RUNNING'),"
                    " (SELECT count(*) FROM plan_nodes WHERE task_id = :t"
                    "   AND cancel_requested_at IS NOT NULL"
                    "   AND status NOT IN ('DONE','FAILED','CANCELLED')),"
                    " (SELECT status FROM research_tasks WHERE id = :t)"
                ),
                {"t": task},
            )
        ).one()
        return (
            counts[0] == 0
            and counts[1] == 0
            and counts[2]
            in (
                "DONE",
                "FAILED",
                "CANCELLED",
            )
        )

    async with _running(worker):
        for _ in range(5):  # 让首批领取起跑（t≈5，无人完成：hang=40）
            clock.advance(1.0)
            await asyncio.sleep(0.02)
        victims = rng.sample(nodes[1:], 5)  # 随机取消 5 棵子树
        for victim in victims:
            ids = await subtree_ids(graph_session, victim.id)
            await request_cancel(
                factory, task_id=task, node_ids=ids, reason="storm", hooks=NullBudgetHooks()
            )
        await _drive_until(clock, _converged, step=5.0)
    final = (
        await graph_session.execute(
            text("SELECT status, count(*) FROM plan_nodes WHERE task_id = :t GROUP BY status"),
            {"t": task},
        )
    ).all()
    by_status = {r[0]: r[1] for r in final}
    assert by_status.get("RUNNING", 0) == 0  # 无悬空 RUNNING（05 §4.4 取消完备）
    assert set(by_status) <= {"DONE", "CANCELLED"}  # 无 PENDING/READY 滞留、无意外 FAILED
    assert by_status.get("CANCELLED", 0) >= 5  # 至少 5 个受害者本体
    task_status = (
        await graph_session.execute(
            text("SELECT status FROM research_tasks WHERE id = :t"), {"t": task}
        )
    ).scalar_one()
    assert task_status == "CANCELLED"
