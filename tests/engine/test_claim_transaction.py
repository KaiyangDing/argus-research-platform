"""M1.6 领取事务测试（M1 册测试清单，+8，全 [pg]）。

夹具直接 INSERT research_tasks / plan_nodes 行——M1 无提交 API，测试造数即口径。
"""

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker
from argus.core.types import NodeId, TaskId
from argus.engine.graph import BudgetRequest, TaskBrief
from argus.engine.ports import NullBudgetHooks
from argus.engine.scheduler import claim_batch
from argus.engine.store import NodeSpec

pytestmark = pytest.mark.pg

_SPEC_JSON = json.dumps(
    NodeSpec(
        brief=TaskBrief(objective="o"),
        budget=BudgetRequest(tokens=100, yuan=Decimal("1.0")),
    ).model_dump(mode="json"),
    ensure_ascii=False,
)

_MK_TASK = text(
    """
    INSERT INTO research_tasks
        (id, title, objective, status, corpus_hash, budget_tokens_cap, budget_yuan_cap,
         requested_by)
    VALUES (:id, 't', 'o', :status, 'sha256:test', 100000, 100.00, 'tester')
    """
)

_MK_NODE = text(
    """
    INSERT INTO plan_nodes
        (id, task_id, plan_version_added, node_type, role, spec, status, priority, ready_at,
         blocked_reason, cancel_requested_at)
    VALUES (:id, :task, 0, 'research', 'researcher', CAST(:spec AS jsonb), :status, :priority,
            :ready_at, :blocked, :cancel_at)
    """
)


async def _seed_task(session: AsyncSession, status: str = "EXECUTING") -> TaskId:
    task_id = TaskId(uuid.uuid4())
    await session.execute(_MK_TASK, {"id": task_id, "status": status})
    await session.commit()
    return task_id


async def _seed_node(
    session: AsyncSession,
    task_id: TaskId,
    *,
    status: str = "READY",
    priority: int = 0,
    ready_at: datetime | None = None,
    blocked: str | None = None,
    cancel_at: datetime | None = None,
) -> NodeId:
    node_id = NodeId(uuid.uuid4())
    await session.execute(
        _MK_NODE,
        {
            "id": node_id,
            "task": task_id,
            "spec": _SPEC_JSON,
            "status": status,
            "priority": priority,
            "ready_at": ready_at if ready_at is not None else datetime.now(UTC),
            "blocked": blocked,
            "cancel_at": cancel_at,
        },
    )
    await session.commit()
    return node_id


async def test_claim_ready_to_running_lease_attempt(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)
    claimed = await claim_batch(
        build_sessionmaker(pg_engine),
        worker_id="w1",
        batch=4,
        lease_ttl_seconds=90,
        hooks=NullBudgetHooks(),
    )
    assert [c.node_id for c in claimed] == [node]
    got = claimed[0]
    assert got.attempt == 1  # 0→1：attempt 只在领取 +1（Z-11）
    assert got.task_id == task
    assert got.max_attempts == 2
    assert got.spec.brief.objective == "o"  # 同事务补查还原 NodeSpec
    row = (
        await graph_session.execute(
            text(
                "SELECT status, lease_owner, lease_expires_at, attempt, started_at "
                "FROM plan_nodes WHERE id = :id"
            ),
            {"id": node},
        )
    ).one()
    assert row.status == "RUNNING"
    assert row.lease_owner == "w1"
    assert row.lease_expires_at is not None
    assert row.attempt == 1
    assert row.started_at is not None  # 冻结区 2.5 #12 追加语句


async def test_claim_skips_blocked_and_cancel_requested(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    ok = await _seed_node(graph_session, task)
    await _seed_node(graph_session, task, blocked="budget")
    await _seed_node(graph_session, task, cancel_at=datetime.now(UTC))
    claimed = await claim_batch(
        build_sessionmaker(pg_engine),
        worker_id="w1",
        batch=10,
        lease_ttl_seconds=90,
        hooks=NullBudgetHooks(),
    )
    # 预算阻塞与已带取消意图的 READY 行不被领取（G.1 WHERE 前两条件）
    assert [c.node_id for c in claimed] == [ok]


async def test_claim_task_status_gate(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    claimable: set[TaskId] = set()
    for status in ["SUBMITTED", "AWAITING_APPROVAL", "DONE", "PLANNING", "EXECUTING"]:
        task = await _seed_task(graph_session, status=status)
        await _seed_node(graph_session, task)
        if status in ("PLANNING", "EXECUTING"):
            claimable.add(task)
    claimed = await claim_batch(
        build_sessionmaker(pg_engine),
        worker_id="w1",
        batch=10,
        lease_ttl_seconds=90,
        hooks=NullBudgetHooks(),
    )
    # 审批/终态任务零派发，PLANNING/EXECUTING 可领（03 §3.1 任务状态门）
    assert {c.task_id for c in claimed} == claimable


async def test_claim_two_sessions_no_double(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    seeded = {await _seed_node(graph_session, task) for _ in range(6)}
    factory = build_sessionmaker(pg_engine)
    a, b = await asyncio.gather(
        claim_batch(
            factory, worker_id="wa", batch=5, lease_ttl_seconds=90, hooks=NullBudgetHooks()
        ),
        claim_batch(
            factory, worker_id="wb", batch=5, lease_ttl_seconds=90, hooks=NullBudgetHooks()
        ),
    )
    ids_a = {c.node_id for c in a}
    ids_b = {c.node_id for c in b}
    assert ids_a & ids_b == set()  # SKIP LOCKED：并发领取零交集
    assert ids_a | ids_b == seeded  # 6 行全部恰被领走一次


async def test_claim_order_priority_then_ready_at(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    base = datetime.now(UTC)
    n_low_old = await _seed_node(graph_session, task, priority=0, ready_at=base)
    n_high = await _seed_node(
        graph_session, task, priority=5, ready_at=base + timedelta(seconds=30)
    )
    n_low_older = await _seed_node(
        graph_session, task, priority=0, ready_at=base - timedelta(seconds=30)
    )
    claimed = await claim_batch(
        build_sessionmaker(pg_engine),
        worker_id="w1",
        batch=2,
        lease_ttl_seconds=90,
        hooks=NullBudgetHooks(),
    )
    # ORDER BY priority DESC, ready_at + LIMIT 2：高优先级先走，同级看 ready_at 早者
    assert {c.node_id for c in claimed} == {n_high, n_low_older}
    assert n_low_old not in {c.node_id for c in claimed}


async def test_claim_started_at_written_once(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)
    factory = build_sessionmaker(pg_engine)
    await claim_batch(
        factory, worker_id="w1", batch=1, lease_ttl_seconds=90, hooks=NullBudgetHooks()
    )
    first = (
        await graph_session.execute(
            text("SELECT started_at FROM plan_nodes WHERE id = :id"), {"id": node}
        )
    ).scalar_one()
    # 模拟 T3b 回队（清租约、回 READY；started_at 保留）
    await graph_session.execute(
        text(
            "UPDATE plan_nodes SET status = 'READY', lease_owner = NULL, "
            "lease_expires_at = NULL WHERE id = :id"
        ),
        {"id": node},
    )
    await graph_session.commit()
    again = await claim_batch(
        factory, worker_id="w2", batch=1, lease_ttl_seconds=90, hooks=NullBudgetHooks()
    )
    assert [c.attempt for c in again] == [2]
    second = (
        await graph_session.execute(
            text("SELECT started_at FROM plan_nodes WHERE id = :id"), {"id": node}
        )
    ).scalar_one()
    assert second == first  # COALESCE：首个 started_at 不被重领改写


async def test_claim_isolation_read_committed(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    # 决定 #16 / ADR-004：领取事务显式 READ COMMITTED——断言 SET 语句在事务内生效
    async with build_sessionmaker(pg_engine)() as session:
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        level = (await session.execute(text("SHOW transaction_isolation"))).scalar_one()
        assert level == "read committed"
        await session.rollback()


async def test_claim_skips_task_under_for_update(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)
    factory = build_sessionmaker(pg_engine)
    holder = factory()
    try:
        # 模拟图手术持任务行 FOR UPDATE 未提交（03 §5.3 互斥语义预演）
        await holder.execute(
            text("SELECT id FROM research_tasks WHERE id = :t FOR UPDATE"), {"t": task}
        )
        during = await claim_batch(
            factory, worker_id="w1", batch=5, lease_ttl_seconds=90, hooks=NullBudgetHooks()
        )
        assert during == []  # 任务门 FOR SHARE SKIP LOCKED：手术中的任务整体跳过
        await holder.rollback()
    finally:
        await holder.close()
    after = await claim_batch(
        factory, worker_id="w1", batch=5, lease_ttl_seconds=90, hooks=NullBudgetHooks()
    )
    assert [c.node_id for c in after] == [node]  # 锁释放后恢复可领
