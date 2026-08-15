"""reaper 三路分流（03 §3.3 / 速查表 G.4；M1 册 M1.11）。

不是独立进程：每个 worker 内置的周期任务（默认 15s），幂等、多副本并跑无害。
三分支 attempt 都不 +1（Z-11）；分支①不结算预算（G.5 置换口径：旧预留由下一次
领取事务原子置换，M2 的 on_claim hook 责任）。分支③依赖 M1.12 的 commit_cancelled，
本步 stub 占位——绝不写成"回 READY"（G.4③ 明令禁止的事故形态）。
"""

from typing import NamedTuple

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.types import NodeId, TaskId
from argus.engine.graph import FailureClass
from argus.engine.lease import ReaperGuard, commit_failed, commit_needs_replan
from argus.engine.ports import BudgetHooks, ReplanSignalDraft


class ReapStats(NamedTuple):
    requeued: int
    terminal_failed: int
    terminal_needs_replan: int
    terminal_cancelled: int


_BRANCH1_SQL = text(  # G.4①【逐字】：常规收回——未被取消、还有重试余量 → 回 READY
    """
    UPDATE plan_nodes
    SET status = 'READY', lease_owner = NULL, lease_expires_at = NULL
    WHERE status = 'RUNNING' AND lease_expires_at < now()
      AND cancel_requested_at IS NULL
      AND attempt < max_attempts
    RETURNING id, purity
    """
)

_BRANCH2_SELECT = text(  # 分支②圈定【实现细则 SQL】：重试耗尽，按 rof 分流 T5/T6
    """
    SELECT id, task_id, spec->>'replan_on_failure' AS rof FROM plan_nodes
    WHERE status = 'RUNNING' AND lease_expires_at < now()
      AND cancel_requested_at IS NULL AND attempt >= max_attempts
    FOR UPDATE SKIP LOCKED
    """
)

_BRANCH3_SELECT = text(  # 分支③圈定【实现细则 SQL】：取消中崩溃——不设 attempt 条件
    """
    SELECT id, task_id FROM plan_nodes
    WHERE status = 'RUNNING' AND lease_expires_at < now()
      AND cancel_requested_at IS NOT NULL
    FOR UPDATE SKIP LOCKED
    """
)

_EXHAUSTED_ERROR = {"cause": "lease_expired_retries_exhausted"}


async def reap_once(
    session_factory: async_sessionmaker[AsyncSession], *, hooks: BudgetHooks
) -> ReapStats:
    """一轮三路分流；影响 0 行 = 已被他人处理，静默继续（幂等可重入）。"""
    requeued = await _reap_requeue(session_factory)
    failed, needs_replan = await _reap_exhausted(session_factory, hooks)
    cancelled = await _reap_cancelled(session_factory, hooks)
    return ReapStats(requeued, failed, needs_replan, cancelled)


async def _reap_requeue(session_factory: async_sessionmaker[AsyncSession]) -> int:
    """分支①：单语句事务（不包长事务，reaper 挂在 worker 循环里，长事务拖累领取）。"""
    async with session_factory() as session:
        rows = (await session.execute(_BRANCH1_SQL)).all()
        await session.commit()
        return len(rows)


async def _reap_exhausted(
    session_factory: async_sessionmaker[AsyncSession], hooks: BudgetHooks
) -> tuple[int, int]:
    """分支②：短事务圈定 → 逐行走 M1.8 终态函数（与 T5/T6 同一代码路径，G.4 原文）。"""
    async with session_factory() as session:
        rows = (await session.execute(_BRANCH2_SELECT)).all()
        await session.commit()  # 释放圈定锁；处置事务靠 ReaperGuard 重验前提
    failed = 0
    needs_replan = 0
    for row in rows:
        if row.rof == "true":
            ok = await commit_needs_replan(
                session_factory,
                task_id=TaskId(row.task_id),
                node_id=NodeId(row.id),
                guard=ReaperGuard(),
                signal=ReplanSignalDraft(kind="node_replan", payload={"cause": "retry_exhausted"}),
                error=_EXHAUSTED_ERROR,
                failure_class=FailureClass.RETRY_EXHAUSTED,
                hooks=hooks,
            )
            needs_replan += 1 if ok else 0
        else:
            ok = await commit_failed(
                session_factory,
                task_id=TaskId(row.task_id),
                node_id=NodeId(row.id),
                guard=ReaperGuard(),
                failure_class=FailureClass.RETRY_EXHAUSTED,
                error=_EXHAUSTED_ERROR,
                signal=None,
                hooks=hooks,
            )
            failed += 1 if ok else 0
    return failed, needs_replan


async def _reap_cancelled(
    session_factory: async_sessionmaker[AsyncSession], hooks: BudgetHooks
) -> int:
    """分支③：取消中崩溃 → 直接 CANCELLED（TODO(M1.12)：commit_cancelled 落地后回填）。"""
    async with session_factory() as session:
        rows = (await session.execute(_BRANCH3_SELECT)).all()
        await session.commit()
    count = 0
    for row in rows:
        count += 1 if await _commit_cancelled_stub(row.task_id, row.id) else 0
    return count


async def _commit_cancelled_stub(task_id: object, node_id: object) -> bool:
    raise NotImplementedError("TODO(M1.12)：commit_cancelled 落地后回填分支③——绝不回 READY（G.4③）")
