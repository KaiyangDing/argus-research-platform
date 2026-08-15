"""M1.11 reaper 三路分流测试（M1 册测试清单，+8，全 [pg]）。

过期一律 `UPDATE ... SET lease_expires_at = now() - interval '1 second'` 制造
（冻结区 2.5 #21：FakeClock 管不了 SQL 的 now()）。分支③用例在 M1.12。
"""

import asyncio
import json
import uuid
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import Row, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker
from argus.core.types import ArtifactKind, NodeId, TaskId
from argus.engine.graph import BudgetRequest, TaskBrief
from argus.engine.lease import WorkerGuard, commit_done
from argus.engine.ports import ArtifactDraft, NullBudgetHooks
from argus.engine.reaper import _BRANCH1_SQL, reap_once
from argus.engine.scheduler import claim_batch
from argus.engine.store import NodeSpec

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
         max_attempts, purity, lease_owner, lease_expires_at, cancel_requested_at)
    VALUES (:id, :task, 0, 'research', 'researcher', CAST(:spec AS jsonb), :status, :attempt,
            :max_attempts, :purity, :owner, now() + CAST(:ttl AS interval), :cancel_at)
    """
)

_EXPIRE = text(
    "UPDATE plan_nodes SET lease_expires_at = now() - interval '1 second' WHERE id = :id"
)


def _spec_json(*, replan_on_failure: bool = False) -> str:
    return json.dumps(
        NodeSpec(
            brief=TaskBrief(objective="o"),
            budget=BudgetRequest(tokens=100, yuan=Decimal("1.0")),
            replan_on_failure=replan_on_failure,
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
    max_attempts: int = 2,
    purity: str = "pure",
    owner: str | None = "w1",
    ttl: timedelta | None = timedelta(seconds=90),
    cancel_now: bool = False,
    replan_on_failure: bool = False,
) -> NodeId:
    node_id = NodeId(uuid.uuid4())
    await session.execute(
        _MK_NODE,
        {
            "id": node_id,
            "task": task_id,
            "spec": _spec_json(replan_on_failure=replan_on_failure),
            "status": status,
            "attempt": attempt,
            "max_attempts": max_attempts,
            "purity": purity,
            "owner": owner,
            "ttl": ttl,
            "cancel_at": None,
        },
    )
    if cancel_now:
        await session.execute(
            text("UPDATE plan_nodes SET cancel_requested_at = now() WHERE id = :id"),
            {"id": node_id},
        )
    await session.commit()
    return node_id


async def _expire(session: AsyncSession, node_id: NodeId) -> None:
    await session.execute(_EXPIRE, {"id": node_id})
    await session.commit()


async def _node_row(session: AsyncSession, node_id: NodeId) -> Row[Any]:
    return (
        await session.execute(
            text(
                "SELECT status, attempt, lease_owner, lease_expires_at, failure_class, "
                "error, finished_at FROM plan_nodes WHERE id = :id"
            ),
            {"id": node_id},
        )
    ).one()


async def test_branch1_requeue_no_attempt_increment(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, attempt=1)
    await _expire(graph_session, node)
    stats = await reap_once(build_sessionmaker(pg_engine), hooks=NullBudgetHooks())
    assert stats.requeued == 1
    assert (stats.terminal_failed, stats.terminal_needs_replan, stats.terminal_cancelled) == (
        0,
        0,
        0,
    )
    row = await _node_row(graph_session, node)
    assert row.status == "READY"
    assert row.lease_owner is None
    assert row.lease_expires_at is None
    assert row.attempt == 1  # reaper 绝不 attempt+1（Z-11）


async def test_branch1_returns_purity(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, purity="effectful")
    await _expire(graph_session, node)
    async with build_sessionmaker(pg_engine)() as session:
        rows = (await session.execute(_BRANCH1_SQL)).all()
        await session.commit()
    # RETURNING id, purity：EFFECTFUL 续跑提示（03 §4.3）——直接执行语句断言形状
    assert [(r.id, r.purity) for r in rows] == [(node, "effectful")]


async def test_branch1_skips_unexpired_and_cancelled(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    normal = await _seed_node(graph_session, task)
    unexpired = await _seed_node(graph_session, task)
    cancelled = await _seed_node(graph_session, task, cancel_now=True)
    await _expire(graph_session, normal)
    await _expire(graph_session, cancelled)
    async with build_sessionmaker(pg_engine)() as session:
        rows = (await session.execute(_BRANCH1_SQL)).all()
        await session.commit()
    assert [r.id for r in rows] == [normal]  # 未过期与带取消意图的行①不碰
    for untouched in (unexpired, cancelled):
        row = await _node_row(graph_session, untouched)
        assert row.status == "RUNNING"
        assert row.lease_owner == "w1"


async def test_branch2_failed_when_replan_false(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, attempt=2, replan_on_failure=False)
    await _expire(graph_session, node)
    stats = await reap_once(build_sessionmaker(pg_engine), hooks=NullBudgetHooks())
    assert stats.terminal_failed == 1
    assert stats.requeued == 0
    row = await _node_row(graph_session, node)
    assert row.status == "FAILED"
    assert row.failure_class == "retry_exhausted"
    assert row.error == {"cause": "lease_expired_retries_exhausted"}
    assert row.finished_at is not None
    assert row.lease_owner is None
    sig = (
        await graph_session.execute(
            text("SELECT kind, status FROM replan_signals WHERE task_id = :t"), {"t": task}
        )
    ).one()
    assert (sig.kind, sig.status) == ("node_replan", "open")


async def test_branch2_needs_replan_when_replan_true(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, attempt=2, replan_on_failure=True)
    await _expire(graph_session, node)
    stats = await reap_once(build_sessionmaker(pg_engine), hooks=NullBudgetHooks())
    assert stats.terminal_needs_replan == 1
    assert stats.terminal_failed == 0
    row = await _node_row(graph_session, node)
    assert row.status == "NEEDS_REPLAN"
    assert row.failure_class == "retry_exhausted"
    assert row.finished_at is None  # 滞留态非终态
    count = (
        await graph_session.execute(
            text("SELECT count(*) FROM replan_signals WHERE task_id = :t"), {"t": task}
        )
    ).scalar_one()
    assert count == 1


async def test_poison_node_converges(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, status="READY", attempt=0, owner=None, ttl=None)
    factory = build_sessionmaker(pg_engine)
    hooks = NullBudgetHooks()
    # 毒节点生命线：领取(a=1)→过期→①回队→领取(a=2)→过期→②终态，attempt 恰 max、不无限烧
    first = await claim_batch(factory, worker_id="w1", batch=1, lease_ttl_seconds=90, hooks=hooks)
    assert [c.attempt for c in first] == [1]
    await _expire(graph_session, node)
    stats1 = await reap_once(factory, hooks=hooks)
    assert stats1.requeued == 1
    second = await claim_batch(factory, worker_id="w2", batch=1, lease_ttl_seconds=90, hooks=hooks)
    assert [c.attempt for c in second] == [2]
    await _expire(graph_session, node)
    stats2 = await reap_once(factory, hooks=hooks)
    assert stats2.terminal_failed == 1
    row = await _node_row(graph_session, node)
    assert row.status == "FAILED"
    assert row.attempt == 2  # 全程恰 max_attempts 次执行
    stats3 = await reap_once(factory, hooks=hooks)  # 幂等：终态后再收割颗粒无收
    assert stats3 == (0, 0, 0, 0)


async def test_two_reapers_concurrent_idempotent(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    nodes = [await _seed_node(graph_session, task, attempt=1) for _ in range(5)]
    for n in nodes:
        await _expire(graph_session, n)
    factory = build_sessionmaker(pg_engine)
    a, b = await asyncio.gather(
        reap_once(factory, hooks=NullBudgetHooks()),
        reap_once(factory, hooks=NullBudgetHooks()),
    )
    assert a.requeued + b.requeued == 5  # 不重复处理：总和恰等于过期行数
    statuses = (
        await graph_session.execute(
            text("SELECT status FROM plan_nodes WHERE task_id = :t"), {"t": task}
        )
    ).scalars()
    assert set(statuses) == {"READY"}


async def test_stale_worker_commit_after_reap_rejected(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, attempt=1, owner="w1")
    await _expire(graph_session, node)
    factory = build_sessionmaker(pg_engine)
    stats = await reap_once(factory, hooks=NullBudgetHooks())
    assert stats.requeued == 1
    payload = {"late": True}
    late = await commit_done(
        factory,
        task_id=task,
        node_id=node,
        guard=WorkerGuard(worker_id="w1", attempt=1),  # 原持有者迟到提交
        artifact=ArtifactDraft(
            kind=ArtifactKind.RESEARCH_NOTE,
            schema_name="s",
            payload=payload,
            headline="h",
            content_hash=ArtifactDraft.hash_payload(payload),
        ),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert late is False  # ①收回后 status 已非 RUNNING：fencing 闭环
    count = (
        await graph_session.execute(
            text("SELECT count(*) FROM artifacts WHERE task_id = :t"), {"t": task}
        )
    ).scalar_one()
    assert count == 0
    row = await _node_row(graph_session, node)
    assert row.status == "READY"
