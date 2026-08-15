"""M1.7 心跳与 T4 完成事务测试（M1 册测试清单，+9，全 [pg]）。"""

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
from argus.engine.lease import WorkerGuard, commit_done, heartbeat
from argus.engine.ports import ArtifactDraft, NullBudgetHooks, ReplanSignalDraft
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
    VALUES (:id, 't', 'o', 'EXECUTING', 'sha256:test', 100000, 100.00, 'tester')
    """
)

_MK_NODE = text(
    """
    INSERT INTO plan_nodes
        (id, task_id, plan_version_added, node_type, role, spec, status, attempt,
         lease_owner, lease_expires_at, cancel_requested_at)
    VALUES (:id, :task, 0, 'research', 'researcher', CAST(:spec AS jsonb), :status, :attempt,
            :owner, now() + CAST(:ttl AS interval), :cancel_at)
    """
)

_MK_EDGE = text(
    """
    INSERT INTO plan_edges (task_id, from_node, to_node, plan_version_added)
    VALUES (:task, :src, :dst, 0)
    """
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
    cancel_at_now: bool = False,
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
            "cancel_at": None,
        },
    )
    if cancel_at_now:
        await session.execute(
            text("UPDATE plan_nodes SET cancel_requested_at = now() WHERE id = :id"),
            {"id": node_id},
        )
    await session.commit()
    return node_id


def _draft(kind: ArtifactKind = ArtifactKind.RESEARCH_NOTE) -> ArtifactDraft:
    payload = {"note": "done"}
    return ArtifactDraft(
        kind=kind,
        schema_name="scripted_note",
        payload=payload,
        headline="h",
        content_hash=ArtifactDraft.hash_payload(payload),
    )


async def _node_row(session: AsyncSession, node_id: NodeId) -> Row[Any]:
    res = await session.execute(
        text(
            "SELECT status, attempt, lease_owner, lease_expires_at, checkpoint_artifact_id, "
            "finished_at, ready_at FROM plan_nodes WHERE id = :id"
        ),
        {"id": node_id},
    )
    return res.one()


async def test_heartbeat_extends_lease(graph_session: AsyncSession, pg_engine: AsyncEngine) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, ttl=timedelta(seconds=10))
    before = (
        await graph_session.execute(
            text("SELECT lease_expires_at FROM plan_nodes WHERE id = :id"), {"id": node}
        )
    ).scalar_one()
    result = await heartbeat(
        build_sessionmaker(pg_engine), node_id=node, worker_id="w1", lease_ttl_seconds=90
    )
    assert result.alive is True
    assert result.cancel_requested is False
    after = (
        await graph_session.execute(
            text("SELECT lease_expires_at FROM plan_nodes WHERE id = :id"), {"id": node}
        )
    ).scalar_one()
    assert after > before  # 续租：到期时刻后移


async def test_heartbeat_zero_rows_when_owner_changed(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, owner="w2")
    result = await heartbeat(
        build_sessionmaker(pg_engine), node_id=node, worker_id="w1", lease_ttl_seconds=90
    )
    # G.2：影响行数=0 意味着租约已不属于我（alive=False 时不再查取消意图）
    assert result == (False, False)


async def test_heartbeat_reports_cancel_requested(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, cancel_at_now=True)
    result = await heartbeat(
        build_sessionmaker(pg_engine), node_id=node, worker_id="w1", lease_ttl_seconds=90
    )
    # 心跳顺带观察取消意图（03 §7.1②；冻结区 2.5 #13）
    assert result == (True, True)


async def test_commit_done_atomic_bundle(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)
    ok = await commit_done(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard(worker_id="w1", attempt=1),
        artifact=_draft(),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert ok is True
    art = (
        await graph_session.execute(
            text("SELECT id, producer_node, kind, content_hash FROM artifacts WHERE task_id = :t"),
            {"t": task},
        )
    ).one()
    assert art.producer_node == node
    assert art.kind == "research_note"
    row = await _node_row(graph_session, node)
    # 02-T3 五件事的 M1 形态：DONE + checkpoint 回填 + 租约清空 + finished_at
    assert row.status == "DONE"
    assert row.checkpoint_artifact_id == art.id
    assert row.lease_owner is None
    assert row.lease_expires_at is None
    assert row.finished_at is not None


async def test_commit_done_promotes_only_last_predecessor(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    b = await _seed_node(graph_session, task, owner="wb")
    c = await _seed_node(graph_session, task, owner="wc")
    d = await _seed_node(graph_session, task, status="PENDING", attempt=0, owner=None, ttl=None)
    for src in (b, c):
        await graph_session.execute(_MK_EDGE, {"task": task, "src": src, "dst": d})
    await graph_session.commit()
    factory = build_sessionmaker(pg_engine)
    assert await commit_done(
        factory,
        task_id=task,
        node_id=b,
        guard=WorkerGuard("wb", 1),
        artifact=_draft(),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    mid = await _node_row(graph_session, d)
    assert mid.status == "PENDING"  # 只有一个前驱 DONE：不促升（03-T1"最后一个前驱"）
    assert mid.ready_at is None
    assert await commit_done(
        factory,
        task_id=task,
        node_id=c,
        guard=WorkerGuard("wc", 1),
        artifact=_draft(),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    fin = await _node_row(graph_session, d)
    assert fin.status == "READY"  # 第二个前驱 DONE：同事务促升
    assert fin.ready_at is not None


async def test_commit_done_writes_signal_row(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)
    assert await commit_done(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 1),
        artifact=_draft(),
        signal=ReplanSignalDraft(kind="coverage_gap"),
        hooks=NullBudgetHooks(),
    )
    row = (
        await graph_session.execute(
            text("SELECT node_id, kind, severity, status FROM replan_signals WHERE task_id = :t"),
            {"t": task},
        )
    ).one()
    # 03 §2.2-T4：信号行与 DONE 同事务；缺省权重 coverage_gap=1
    assert row.node_id == node
    assert row.kind == "coverage_gap"
    assert row.severity == 1
    assert row.status == "open"


async def test_commit_done_fencing_stale_attempt(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, attempt=2)  # 已被重领：库里 attempt=2
    ok = await commit_done(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", attempt=1),  # 旧 worker 拿着 attempt=1 迟到提交
        artifact=_draft(),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert ok is False
    count = (
        await graph_session.execute(
            text("SELECT count(*) FROM artifacts WHERE task_id = :t"), {"t": task}
        )
    ).scalar_one()
    assert count == 0  # 回滚彻底：②的工件行随之消失（03 §3.3 fencing）
    row = await _node_row(graph_session, node)
    assert row.status == "RUNNING"
    assert row.attempt == 2


async def test_commit_done_fencing_wrong_owner(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task, owner="w2")
    ok = await commit_done(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard(worker_id="w1", attempt=1),
        artifact=_draft(),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert ok is False
    row = await _node_row(graph_session, node)
    assert row.status == "RUNNING"
    assert row.checkpoint_artifact_id is None
    assert row.lease_owner == "w2"


async def test_task_finalized_done_with_report_final(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task = await _seed_task(graph_session)
    node = await _seed_node(graph_session, task)  # 单节点任务：它就是 sink
    assert await commit_done(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 1),
        artifact=_draft(kind=ArtifactKind.REPORT_FINAL),
        signal=None,
        hooks=NullBudgetHooks(),
    )
    row = (
        await graph_session.execute(
            text("SELECT status, finished_at FROM research_tasks WHERE id = :t"), {"t": task}
        )
    ).one()
    # 冻结区 2.5 #14 分支 A：全部 DONE 且存在 report_final → 任务 DONE
    assert row.status == "DONE"
    assert row.finished_at is not None
