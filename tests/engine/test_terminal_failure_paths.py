"""M1.8 失败收尾 T3b/T5/T6 与判定表测试（M1 册测试清单，+8；判定表两条纯单测，其余 [pg]）。"""

import json
import uuid
from datetime import timedelta
from decimal import Decimal

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from argus.core.db import build_sessionmaker
from argus.core.types import ArtifactKind, NodeId, TaskId
from argus.engine.graph import BudgetRequest, FailureClass, TaskBrief
from argus.engine.lease import (
    ReaperGuard,
    TerminalKind,
    WorkerGuard,
    commit_failed,
    commit_needs_replan,
    commit_retry,
    decide_terminal,
)
from argus.engine.ports import (
    ArtifactDraft,
    NodeOutcome,
    NullBudgetHooks,
    OutcomeDegraded,
    OutcomeFailure,
    OutcomeSuccess,
    ReplanSignalDraft,
)
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
    VALUES (:id, 't', 'o', 'EXECUTING', 'sha256:test', 100000, 100.00, 'tester')
    """
)

_MK_NODE = text(
    """
    INSERT INTO plan_nodes
        (id, task_id, plan_version_added, node_type, role, spec, status, attempt,
         lease_owner, lease_expires_at)
    VALUES (:id, :task, 0, 'research', 'researcher', CAST(:spec AS jsonb), 'RUNNING', :attempt,
            :owner, now() + CAST(:ttl AS interval))
    """
)


async def _seed(
    session: AsyncSession,
    *,
    attempt: int = 1,
    owner: str = "w1",
    ttl: timedelta = timedelta(seconds=90),
) -> tuple[TaskId, NodeId]:
    task_id = TaskId(uuid.uuid4())
    node_id = NodeId(uuid.uuid4())
    await session.execute(_MK_TASK, {"id": task_id})
    await session.execute(
        _MK_NODE,
        {
            "id": node_id,
            "task": task_id,
            "spec": _SPEC_JSON,
            "attempt": attempt,
            "owner": owner,
            "ttl": ttl,
        },
    )
    await session.commit()
    return task_id, node_id


def _ok() -> OutcomeSuccess:
    payload = {"note": "ok"}
    return OutcomeSuccess(
        artifact=ArtifactDraft(
            kind=ArtifactKind.RESEARCH_NOTE,
            schema_name="s",
            payload=payload,
            headline="h",
            content_hash=ArtifactDraft.hash_payload(payload),
        )
    )


def _degraded() -> OutcomeDegraded:
    payload = {"note": "partial"}
    return OutcomeDegraded(
        artifact=ArtifactDraft(
            kind=ArtifactKind.RESEARCH_NOTE,
            schema_name="s",
            payload=payload,
            headline="h",
            partial=True,
            content_hash=ArtifactDraft.hash_payload(payload),
        )
    )


def _fail(*, retryable: bool, with_signal: bool = False) -> OutcomeFailure:
    return OutcomeFailure(
        error={"cause": "boom"},
        retryable=retryable,
        replan_signal=ReplanSignalDraft(kind="node_replan") if with_signal else None,
    )


def test_decide_terminal_full_matrix() -> None:
    # (outcome, attempt, max_attempts, replan_on_failure, cancel_requested, 期望)
    cases: list[tuple[NodeOutcome, int, int, bool, bool, TerminalKind]] = [
        # 取消压倒一切（冻结区 2.5 #25），对任何 outcome 变体成立
        (_ok(), 1, 2, False, True, TerminalKind.CANCELLED),
        (_degraded(), 1, 2, False, True, TerminalKind.CANCELLED),
        (_fail(retryable=True), 1, 2, True, True, TerminalKind.CANCELLED),
        # 成功系 → DONE
        (_ok(), 1, 2, False, False, TerminalKind.DONE),
        (_degraded(), 2, 2, True, False, TerminalKind.DONE),
        # T6a：信号压倒重试（含 retryable=False 也压倒）
        (_fail(retryable=True, with_signal=True), 1, 2, False, False, TerminalKind.NEEDS_REPLAN),
        (_fail(retryable=False, with_signal=True), 1, 2, False, False, TerminalKind.NEEDS_REPLAN),
        # T3b：可重试且未耗尽
        (_fail(retryable=True), 1, 2, False, False, TerminalKind.RETRY),
        # 耗尽：T5 或 T6b 按 replan_on_failure
        (_fail(retryable=True), 2, 2, False, False, TerminalKind.FAILED),
        (_fail(retryable=True), 2, 2, True, False, TerminalKind.NEEDS_REPLAN),
        # 不可重试 → T5（replan_on_failure 与 attempt 无关紧要）
        (_fail(retryable=False), 1, 2, True, False, TerminalKind.FAILED),
    ]
    for outcome, attempt, max_attempts, rof, cancel, expected in cases:
        got = decide_terminal(
            outcome,
            attempt=attempt,
            max_attempts=max_attempts,
            replan_on_failure=rof,
            cancel_requested=cancel,
        )
        assert got is expected, f"case {outcome!r} a={attempt}/{max_attempts} rof={rof}: {got}"


def test_decide_terminal_signal_beats_retry() -> None:
    # 明明还有重试额度（attempt<max 且 retryable），但带信号 → T6a 优先（冻结区 2.5 #24）
    got = decide_terminal(
        _fail(retryable=True, with_signal=True),
        attempt=1,
        max_attempts=2,
        replan_on_failure=False,
        cancel_requested=False,
    )
    assert got is TerminalKind.NEEDS_REPLAN


async def test_retry_back_to_ready_with_error(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed(graph_session)
    factory = build_sessionmaker(pg_engine)
    ok = await commit_retry(
        factory,
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 1),
        error={"cause": "flaky"},
    )
    assert ok is True
    row = (
        await graph_session.execute(
            text(
                "SELECT status, attempt, lease_owner, lease_expires_at, error, finished_at "
                "FROM plan_nodes WHERE id = :id"
            ),
            {"id": node},
        )
    ).one()
    assert row.status == "READY"
    assert row.attempt == 1  # T3b 不动 attempt（Z-11）
    assert row.lease_owner is None
    assert row.lease_expires_at is None
    assert row.error == {"cause": "flaky"}  # 失败现场落库
    assert row.finished_at is None
    reclaimed = await claim_batch(
        factory, worker_id="w2", batch=1, lease_ttl_seconds=90, hooks=NullBudgetHooks()
    )
    assert [c.node_id for c in reclaimed] == [node]  # 可被再次领取
    assert reclaimed[0].attempt == 2  # 重领时才 +1


async def test_failed_writes_class_and_auto_signal(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed(graph_session, attempt=2)
    ok = await commit_failed(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 2),
        failure_class=FailureClass.RETRY_EXHAUSTED,
        error={"cause": "still boom"},
        signal=None,  # engine 自动生成信号（03-T5"向重规划器发信号"）
        hooks=NullBudgetHooks(),
    )
    assert ok is True
    row = (
        await graph_session.execute(
            text(
                "SELECT status, failure_class, finished_at, lease_owner "
                "FROM plan_nodes WHERE id = :id"
            ),
            {"id": node},
        )
    ).one()
    assert row.status == "FAILED"
    assert row.failure_class == "retry_exhausted"
    assert row.finished_at is not None
    assert row.lease_owner is None
    sig = (
        await graph_session.execute(
            text("SELECT kind, severity, payload, status FROM replan_signals WHERE task_id = :t"),
            {"t": task},
        )
    ).one()
    assert sig.kind == "node_replan"
    assert sig.severity == 2
    assert sig.payload == {"cause": "retry_exhausted"}
    assert sig.status == "open"


async def test_failed_nonretryable_uses_given_class(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed(graph_session)
    ok = await commit_failed(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 1),
        failure_class=FailureClass.TOOL_ERROR,
        error={"cause": "bad tool"},
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert ok is True
    row = (
        await graph_session.execute(
            text("SELECT failure_class, error FROM plan_nodes WHERE id = :id"), {"id": node}
        )
    ).one()
    assert row.failure_class == "tool_error"  # 给定分类原样落库
    assert row.error == {"cause": "bad tool"}


async def test_needs_replan_t6a_no_output(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed(graph_session)
    ok = await commit_needs_replan(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 1),
        signal=ReplanSignalDraft(kind="node_replan"),
        error=None,
        failure_class=None,
        hooks=NullBudgetHooks(),
    )
    assert ok is True
    row = (
        await graph_session.execute(
            text(
                "SELECT status, failure_class, error, finished_at, lease_owner "
                "FROM plan_nodes WHERE id = :id"
            ),
            {"id": node},
        )
    ).one()
    assert row.status == "NEEDS_REPLAN"
    assert row.failure_class is None  # T6a：None/None
    assert row.error is None
    assert row.finished_at is None  # 滞留态非终态：不写 finished_at
    assert row.lease_owner is None
    counts = (
        await graph_session.execute(
            text(
                "SELECT (SELECT count(*) FROM artifacts WHERE task_id = :t),"
                " (SELECT count(*) FROM replan_signals WHERE task_id = :t)"
            ),
            {"t": task},
        )
    ).one()
    assert counts == (0, 1)  # 无工件、信号行必写


async def test_needs_replan_t6b_retry_exhausted(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed(graph_session, attempt=2)
    ok = await commit_needs_replan(
        build_sessionmaker(pg_engine),
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", 2),
        signal=ReplanSignalDraft(kind="node_replan", payload={"cause": "retry_exhausted"}),
        error={"cause": "lease_expired_retries_exhausted"},
        failure_class=FailureClass.RETRY_EXHAUSTED,
        hooks=NullBudgetHooks(),
    )
    assert ok is True
    row = (
        await graph_session.execute(
            text("SELECT status, failure_class, finished_at FROM plan_nodes WHERE id = :id"),
            {"id": node},
        )
    ).one()
    assert row.status == "NEEDS_REPLAN"
    assert row.failure_class == "retry_exhausted"  # T6b：重试明细随行
    assert row.finished_at is None


async def test_terminal_guard_zero_rows_no_side_effects(
    graph_session: AsyncSession, pg_engine: AsyncEngine
) -> None:
    task, node = await _seed(graph_session, attempt=2)
    factory = build_sessionmaker(pg_engine)
    stale = await commit_failed(
        factory,
        task_id=task,
        node_id=node,
        guard=WorkerGuard("w1", attempt=1),  # 旧 attempt：fencing 拦截
        failure_class=FailureClass.TOOL_ERROR,
        error={"cause": "late"},
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert stale is False
    unexpired = await commit_failed(
        factory,
        task_id=task,
        node_id=node,
        guard=ReaperGuard(),  # 租约未过期：reaper 前置守卫拦截
        failure_class=FailureClass.RETRY_EXHAUSTED,
        error={"cause": "not yet"},
        signal=None,
        hooks=NullBudgetHooks(),
    )
    assert unexpired is False
    row = (
        await graph_session.execute(
            text("SELECT status, failure_class, finished_at FROM plan_nodes WHERE id = :id"),
            {"id": node},
        )
    ).one()
    assert row.status == "RUNNING"  # 节点纹丝不动
    assert row.failure_class is None
    assert row.finished_at is None
    sig_count = (
        await graph_session.execute(
            text("SELECT count(*) FROM replan_signals WHERE task_id = :t"), {"t": task}
        )
    ).scalar_one()
    assert sig_count == 0  # 同事务回滚：无信号行残留
