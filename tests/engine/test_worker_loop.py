"""M1.10 worker 主循环测试（M1 册测试清单，+6；1-5 [pg]，进程内 worker + FakeClock 驱动）。"""

import asyncio
import random
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker
from argus.core.types import ArtifactKind, ContractId, NodeId, RoleName, TaskId
from argus.engine.graph import (
    BudgetRequest,
    PlanEdge,
    PlanGraph,
    PlanNode,
    Purity,
    TaskBrief,
)
from argus.engine.ports import NullBudgetHooks
from argus.engine.scheduler import compute_batch
from argus.engine.store import persist_graph
from argus.engine.worker import EngineTuning, Worker
from tests.support.fake_clock import FakeClock
from tests.support.scripted_executor import ScriptedExecutor, ScriptEntry, ScriptTable
from tests.support.scripted_registry import ScriptedRegistry

_MK_TASK = text(
    """
    INSERT INTO research_tasks
        (id, title, objective, status, corpus_hash, budget_tokens_cap, budget_yuan_cap,
         requested_by)
    VALUES (:id, 't', 'o', 'EXECUTING', 'sha256:test', 100000, 100.00, 'tester')
    """
)


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
    concurrency: int = 8,
    seed: int = 7,
) -> Worker:
    return Worker(
        session_factory=build_sessionmaker(pg_engine),
        registry=ScriptedRegistry(ScriptedExecutor(script, seed=seed)),
        clock=clock,
        rng=random.Random(1),
        tuning=EngineTuning(),
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
    """推虚拟时钟 + 让真实 IO 跑；不收敛即失败而非挂死（wait_for 精神）。"""
    for _ in range(max_iters):
        if await predicate():
            return
        clock.advance(step)
        await asyncio.sleep(0.02)
    raise AssertionError(f"worker did not converge within {max_iters} iters")


async def _statuses(session: AsyncSession, task: TaskId) -> dict[NodeId, str]:
    res = await session.execute(
        text("SELECT id, status FROM plan_nodes WHERE task_id = :t"), {"t": task}
    )
    return {NodeId(r.id): r.status for r in res}


async def _running_count(session: AsyncSession, task: TaskId) -> int:
    res = await session.execute(
        text("SELECT count(*) FROM plan_nodes WHERE task_id = :t AND status = 'RUNNING'"),
        {"t": task},
    )
    return int(res.scalar_one())


@pytest.mark.pg
async def test_linear_chain_runs_to_task_done(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    a, b = _node(), _node()
    c = _node(RoleName.SYNTHESIZER)
    task = TaskId(uuid.uuid4())
    graph = PlanGraph(
        task_id=task,
        version=0,
        nodes=(a, b, c),
        edges=(
            PlanEdge(src=a.id, dst=b.id, contract=ContractId("research_memo@1")),
            PlanEdge(src=b.id, dst=c.id, contract=ContractId("research_memo@1")),
        ),
    )
    await _persist(graph_session, graph)
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (c.id, 1): ScriptEntry(action="success", artifact_kind=ArtifactKind.REPORT_FINAL),
    }
    worker = _worker(pg_engine, clock, script)

    async def _task_done() -> bool:
        res = await graph_session.execute(
            text("SELECT status FROM research_tasks WHERE id = :t"), {"t": task}
        )
        return str(res.scalar_one()) == "DONE"

    async with _running(worker):
        await _drive_until(clock, _task_done)
    statuses = await _statuses(graph_session, task)
    assert set(statuses.values()) == {"DONE"}  # 链上三节点全 DONE
    rows = await graph_session.execute(
        text("SELECT producer_node, count(*) FROM artifacts WHERE task_id = :t GROUP BY 1"),
        {"t": task},
    )
    per_node = {NodeId(r[0]): r[1] for r in rows}
    assert per_node == {a.id: 1, b.id: 1, c.id: 1}  # 每节点恰一行工件


@pytest.mark.pg
async def test_diamond_join_waits_both(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    a = _node(RoleName.PLANNER)
    b, c = _node(), _node()
    d = _node(RoleName.SYNTHESIZER)
    task = TaskId(uuid.uuid4())
    graph = PlanGraph(
        task_id=task,
        version=0,
        nodes=(a, b, c, d),
        edges=(
            PlanEdge(src=a.id, dst=b.id, contract=ContractId("research_brief@1")),
            PlanEdge(src=a.id, dst=c.id, contract=ContractId("research_brief@1")),
            PlanEdge(src=b.id, dst=d.id, contract=ContractId("research_memo@1")),
            PlanEdge(src=c.id, dst=d.id, contract=ContractId("research_memo@1")),
        ),
    )
    await _persist(graph_session, graph)
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (c.id, 1): ScriptEntry(action="hang", hang_seconds=500),  # c 慢：拖住汇合点
        (d.id, 1): ScriptEntry(action="success", artifact_kind=ArtifactKind.REPORT_FINAL),
    }
    worker = _worker(pg_engine, clock, script)

    async def _b_done() -> bool:
        return (await _statuses(graph_session, task)).get(b.id) == "DONE"

    async def _all_done() -> bool:
        statuses = await _statuses(graph_session, task)
        return set(statuses.values()) == {"DONE"}

    async with _running(worker):
        await _drive_until(clock, _b_done, max_iters=100)
        mid = await _statuses(graph_session, task)
        # 一个前驱（b）DONE、另一个（c）还挂着：汇合点 d 绝不能被促升/领取（05 §4.4 菱形）
        assert mid[c.id] == "RUNNING"
        assert mid[d.id] == "PENDING"
        clock.advance(600)  # 放行 c 的 hang
        await _drive_until(clock, _all_done)


@pytest.mark.pg
async def test_semaphore_caps_inflight(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    nodes = tuple(_node() for _ in range(4))
    task = TaskId(uuid.uuid4())
    graph = PlanGraph(task_id=task, version=0, nodes=nodes, edges=())
    await _persist(graph_session, graph)  # 四个独立节点：全部初始促升 READY
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="hang", hang_seconds=200) for n in nodes
    }
    worker = _worker(pg_engine, clock, script, concurrency=2)
    peak = 0
    async with _running(worker):
        for _ in range(30):  # 采样窗口：任意时刻 RUNNING ≤ C=2
            count = await _running_count(graph_session, task)
            peak = max(peak, count)
            assert count <= 2
            clock.advance(1.0)
            await asyncio.sleep(0.02)
        assert peak == 2  # 上限被真实打满过（不是没派活的假绿）

        async def _all_done() -> bool:
            statuses = await _statuses(graph_session, task)
            return set(statuses.values()) == {"DONE"}

        await _drive_until(clock, _all_done, step=100)


@pytest.mark.pg
async def test_batch_adaptive(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    # 纯单测部分：clamp(C − 在跑, 0, C)（冻结区 2.5 #15）
    assert compute_batch(4, 0) == 4
    assert compute_batch(4, 3) == 1
    assert compute_batch(4, 4) == 0
    assert compute_batch(4, 5) == 0
    assert compute_batch(8, 2) == 6
    # 集成侧：C=4、6 个 READY 长任务 → RUNNING 恒 ≤ 4（首轮领满后 batch=0）
    nodes = tuple(_node() for _ in range(6))
    task = TaskId(uuid.uuid4())
    await _persist(graph_session, PlanGraph(task_id=task, version=0, nodes=nodes, edges=()))
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="hang", hang_seconds=10_000) for n in nodes
    }
    worker = _worker(pg_engine, clock, script, concurrency=4)
    peak = 0
    async with _running(worker):
        for _ in range(25):
            count = await _running_count(graph_session, task)
            peak = max(peak, count)
            assert count <= 4
            clock.advance(1.0)
            await asyncio.sleep(0.02)
    assert peak == 4


@pytest.mark.pg
async def test_retryable_failure_then_success(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    n = _node()
    task = TaskId(uuid.uuid4())
    await _persist(graph_session, PlanGraph(task_id=task, version=0, nodes=(n,), edges=()))
    clock = FakeClock(datetime(2026, 1, 1, tzinfo=UTC))
    script: dict[tuple[NodeId, int], ScriptEntry] = {
        (n.id, 1): ScriptEntry(action="fail_retryable"),
        (n.id, 2): ScriptEntry(action="success"),
    }
    worker = _worker(pg_engine, clock, script)

    async def _node_done() -> bool:
        return (await _statuses(graph_session, task)).get(n.id) == "DONE"

    async with _running(worker):
        await _drive_until(clock, _node_done)
    row = (
        await graph_session.execute(
            text("SELECT attempt, error FROM plan_nodes WHERE id = :id"), {"id": n.id}
        )
    ).one()
    assert row.attempt == 2  # 第二次领取成功收尾
    assert row.error == {"cause": "scripted_retryable"}  # 第一轮失败现场曾落库且保留


async def test_no_network_guard_active() -> None:
    # 毕业验收第 5 行的机制性用例：engine 测试进程内任何 httpx 出网即抛
    req = httpx.Request("GET", "http://dashscope.aliyuncs.com/api")
    with pytest.raises(RuntimeError, match="must not touch network"):
        httpx.Client().send(req)
    async_client = httpx.AsyncClient()
    with pytest.raises(RuntimeError, match="must not touch network"):
        await async_client.send(req)
