"""租约维护与成功收尾：G.2 心跳 + G.3 fencing 完成事务（03 §3.3/§2.2；M1 册 M1.7）。

心跳按 G.2 原文（无 attempt 条件——冲突 C-1 按 03 施工，提交守卫兜底）。
终态事务锁序统一：任务行 FOR SHARE → 节点行守卫 UPDATE（G.3 注；与手术 FOR UPDATE 互斥）。
失败路径 M1.8 追加；取消路径 M1.12 追加。
"""

import functools
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any, NamedTuple, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.types import ArgusError, NodeId, TaskId
from argus.engine.graph import FailureClass, NodeStatus
from argus.engine.ports import (
    ArtifactDraft,
    BudgetHooks,
    NodeOutcome,
    OutcomeDegraded,
    OutcomeFailure,
    OutcomeSuccess,
    ReplanSignalDraft,
)


@dataclass(frozen=True)
class WorkerGuard:
    """fencing：worker 路径——G.3 三重守卫的参数载体（attempt 即 token）。"""

    worker_id: str
    attempt: int


@dataclass(frozen=True)
class ReaperGuard:
    """前置状态守卫：reaper 路径（M1.11 用——按过期事实接管，无 owner/attempt 可验）。"""


Guard = WorkerGuard | ReaperGuard


class HeartbeatResult(NamedTuple):
    alive: bool  # False = 租约已不属于我 → 取消本地任务、丢弃结果、绝不提交
    cancel_requested: bool


def _rowcount(res: Result[Any]) -> int:
    """DML 影响行数。AsyncSession.execute 静态签名返回 Result（无 rowcount），
    运行期 DML 实际返回 CursorResult——这里收窄的是类型标注缝隙，不是运行时转换。"""
    return cast(CursorResult[Any], res).rowcount


_DEADLOCK_PGCODE = "40P01"


def _retry_on_deadlock[**P, R](fn: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """C-7 族③：死锁受害者重试（至多 3 次）。

    多行节点 UPDATE 已按 id 全局序预锁降频；残余环（G.3 逐字守卫先锁自有行，
    与子行不保序）交给 PG 死锁检测裁决——受害者事务已整体回滚、守卫幂等，
    整函数重试安全。非死锁的 DBAPIError 原样上抛。"""

    @functools.wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        for attempt in range(3):
            try:
                return await fn(*args, **kwargs)
            except DBAPIError as exc:
                if getattr(exc.orig, "pgcode", None) != _DEADLOCK_PGCODE or attempt == 2:
                    raise
        raise AssertionError("unreachable")

    return wrapper


_HEARTBEAT_SQL = text(  # G.2 原文【逐字】
    """
    UPDATE plan_nodes
    SET lease_expires_at = now() + :lease_ttl
    WHERE id = :node_id AND lease_owner = :worker_id AND status = 'RUNNING'
    """
)

_CANCEL_FLAG_SQL = text(
    "SELECT cancel_requested_at IS NOT NULL FROM plan_nodes WHERE id = :node_id"
)

_LOCK_TASK_SQL = text("SELECT 1 FROM research_tasks WHERE id = :task_id FOR SHARE")

_INSERT_ARTIFACT_SQL = text(
    """
    INSERT INTO artifacts (task_id, producer_node, kind, schema_name, schema_version,
                           payload, headline, digest, refs, topic_keys, supersedes,
                           partial, token_count, content_hash)
    VALUES (:task_id, :producer, :kind, :schema_name, :schema_version,
            CAST(:payload AS jsonb), :headline, :digest, CAST(:refs AS jsonb), :topic_keys,
            NULL, :partial, :token_count, :content_hash)
    RETURNING id
    """
)

_DONE_WORKER_SQL = text(  # WHERE 三条件【逐字 G.3】；SET 按迁移扩展 finished_at（决定 #12）
    """
    UPDATE plan_nodes
    SET status = 'DONE', checkpoint_artifact_id = :aid, lease_owner = NULL,
        lease_expires_at = NULL, finished_at = now()
    WHERE id = :node_id
      AND lease_owner = :worker_id
      AND attempt = :attempt
      AND status = 'RUNNING'
    """
)

_PROMOTE_T1_SQL = text(  # 03-T1：最后一个前驱落 DONE 才促升，同事务（M1 册【实现细则】）
    # 候选先按 id 全局序 FOR UPDATE 预锁（C-7 族③）：多行促升与取消链/其他促升
    # 共用同一加锁序，节点行间环等待在结构上不可能；语义与 03-T1 原式逐字等价
    """
    UPDATE plan_nodes SET status = 'READY', ready_at = now()
    WHERE id IN (
        SELECT p.id FROM plan_nodes p
        WHERE p.status = 'PENDING'
          AND p.id IN (SELECT e.to_node FROM plan_edges e WHERE e.from_node = :done_node)
          AND NOT EXISTS (
              SELECT 1 FROM plan_edges e2
              JOIN plan_nodes p2 ON p2.id = e2.from_node
              WHERE e2.to_node = p.id AND p2.status <> 'DONE')
        ORDER BY p.id
        FOR UPDATE
    )
    """
)

_INSERT_SIGNAL_SQL = text(
    """
    INSERT INTO replan_signals (task_id, node_id, kind, severity, payload)
    VALUES (:task_id, :node_id, :kind, :severity, CAST(:payload AS jsonb))
    """
)

_FINALIZE_DONE_SQL = text(  # 冻结区 2.5 #14 分支 A：全部 DONE 且存在 report_final
    """
    UPDATE research_tasks SET status = 'DONE', finished_at = now()
    WHERE id = :task_id AND status = 'EXECUTING'
      AND NOT EXISTS (SELECT 1 FROM plan_nodes
                      WHERE task_id = :task_id AND status <> 'DONE')
      AND EXISTS (SELECT 1 FROM artifacts
                  WHERE task_id = :task_id AND kind = 'report_final')
    """
)

_FINALIZE_CANCELLED_SQL = text(  # 分支 B：全部终态且存在任一取消意图
    """
    UPDATE research_tasks SET status = 'CANCELLED', finished_at = now()
    WHERE id = :task_id AND status IN ('PLANNING','EXECUTING')
      AND NOT EXISTS (SELECT 1 FROM plan_nodes
                      WHERE task_id = :task_id
                        AND status NOT IN ('DONE','FAILED','CANCELLED'))
      AND EXISTS (SELECT 1 FROM plan_nodes
                  WHERE task_id = :task_id AND cancel_requested_at IS NOT NULL)
    """
)


async def heartbeat(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    node_id: NodeId,
    worker_id: str,
    lease_ttl_seconds: int,
) -> HeartbeatResult:
    """G.2 续租 + 同连接顺带查取消意图（冻结区 2.5 #13；03 §7.1②）。"""
    async with session_factory() as session:
        res = await session.execute(
            _HEARTBEAT_SQL,
            {
                "node_id": node_id,
                "worker_id": worker_id,
                "lease_ttl": timedelta(seconds=lease_ttl_seconds),
            },
        )
        if _rowcount(res) == 0:
            await session.commit()
            return HeartbeatResult(alive=False, cancel_requested=False)
        flag = (await session.execute(_CANCEL_FLAG_SQL, {"node_id": node_id})).scalar_one()
        await session.commit()
        return HeartbeatResult(alive=True, cancel_requested=bool(flag))


def _artifact_params(task_id: TaskId, node_id: NodeId, draft: ArtifactDraft) -> dict[str, Any]:
    """draft 不透明透传（C-2 临时口径：engine 不解析 payload、不生成摘要）。"""
    return {
        "task_id": task_id,
        "producer": node_id,
        "kind": draft.kind.value,
        "schema_name": draft.schema_name,
        "schema_version": draft.schema_version,
        "payload": json.dumps(draft.payload, ensure_ascii=False),
        "headline": draft.headline,
        "digest": draft.digest,
        "refs": json.dumps(list(draft.refs), ensure_ascii=False),
        "topic_keys": list(draft.topic_keys),
        "partial": draft.partial,
        "token_count": draft.token_count,
        "content_hash": draft.content_hash,
    }


async def _maybe_finalize_task(session: AsyncSession, task_id: TaskId) -> None:
    """任务收尾判定（冻结区 2.5 #14）：A 先 B 后；A 命中后 B 被状态门自然挡掉。"""
    await session.execute(_FINALIZE_DONE_SQL, {"task_id": task_id})
    await session.execute(_FINALIZE_CANCELLED_SQL, {"task_id": task_id})


async def _finalize_after_commit(
    session_factory: async_sessionmaker[AsyncSession], task_id: TaskId
) -> None:
    """C-7 临时口径：并发终态的收尾判定竞态补刀。

    两个终态事务并发时各持任务行 FOR SHARE（兼容），事务内判定在 READ COMMITTED
    快照里互看不到对方未提交的终态 → 双双空过 → 任务永久 EXECUTING。
    提交后以独立小事务重跑幂等判定：最后提交者的补刀必见全部已提交终态。
    残余窗口（commit 与补刀间进程崩溃）待主控裁决兜底方案。"""
    async with session_factory() as session:
        await _maybe_finalize_task(session, task_id)
        await session.commit()


async def _promote_and_finalize_after_commit(
    session_factory: async_sessionmaker[AsyncSession], task_id: TaskId, done_node: NodeId
) -> None:
    """C-7 同族：并发 DONE 的 T1 促升竞态补刀（commit_done 专用）。

    菱形汇合点的两个父并发完成时，各自事务内的促升 NOT EXISTS 都看到对方仍
    非 DONE → 双双不促升 → 汇合点永久 PENDING（undone_parents=0 却无人管）。
    提交后重跑幂等促升：最后提交的父必见全部已提交 DONE。促升在前、收尾判定
    在后（刚促出的 READY 会正确阻止任务提前收尾）。"""
    async with session_factory() as session:
        await session.execute(_PROMOTE_T1_SQL, {"done_node": done_node})
        await _maybe_finalize_task(session, task_id)
        await session.commit()


@_retry_on_deadlock
async def commit_done(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    node_id: NodeId,
    guard: Guard,
    artifact: ArtifactDraft,
    signal: ReplanSignalDraft | None,
    hooks: BudgetHooks,
) -> bool:
    """03-T4 单事务；False = 守卫拦截（fencing），调用方丢弃本地结果。

    任何一步异常 → 整体回滚，节点保持 RUNNING 等租约过期重来（03-T4 事务边界）。
    """
    if not isinstance(guard, WorkerGuard):
        raise ArgusError("commit_done only has a worker path (reaper never produces DONE, G.4)")
    async with session_factory() as session:
        # ① 锁序：任务行 FOR SHARE 先行（与手术 FOR UPDATE 互斥）
        await session.execute(_LOCK_TASK_SQL, {"task_id": task_id})
        # ② 工件不透明落库，拿 id
        aid = (
            await session.execute(
                _INSERT_ARTIFACT_SQL, _artifact_params(task_id, node_id, artifact)
            )
        ).scalar_one()
        # ③ 守卫 UPDATE：WHERE 逐字 G.3；rowcount=0 → 回滚（②随之消失）
        res = await session.execute(
            _DONE_WORKER_SQL,
            {
                "node_id": node_id,
                "worker_id": guard.worker_id,
                "attempt": guard.attempt,
                "aid": aid,
            },
        )
        if _rowcount(res) == 0:
            await session.rollback()
            return False
        # ④ T1 促升（同事务：DONE 与后继就绪原子可见）
        await session.execute(_PROMOTE_T1_SQL, {"done_node": node_id})
        # ⑤ 信号行（03 §2.2-T4 同事务）
        if signal is not None:
            await session.execute(
                _INSERT_SIGNAL_SQL,
                {
                    "task_id": task_id,
                    "node_id": node_id,
                    "kind": signal.kind,
                    "severity": signal.severity,
                    "payload": json.dumps(signal.payload, ensure_ascii=False),
                },
            )
        # ⑥ 预算挂点（M2: SETTLE + RELEASE）
        await hooks.on_terminal(
            session,
            task_id=task_id,
            node_id=node_id,
            attempt=guard.attempt,
            terminal=NodeStatus.DONE,
        )
        # ⑦ 任务收尾判定
        await _maybe_finalize_task(session, task_id)
        await session.commit()
    # C-7 竞态补刀（促升 + 收尾判定，见 _promote_and_finalize_after_commit docstring）
    await _promote_and_finalize_after_commit(session_factory, task_id, node_id)
    return True


class TerminalKind(StrEnum):
    """outcome→迁移判定结果（M1 册【实现细则】）。"""

    RETRY = "retry"  # T3b：回 READY
    DONE = "done"  # T4
    FAILED = "failed"  # T5
    NEEDS_REPLAN = "needs_replan"  # T6
    CANCELLED = "cancelled"  # T7（M1.12 触发）


def decide_terminal(
    outcome: NodeOutcome,
    *,
    attempt: int,
    max_attempts: int,
    replan_on_failure: bool,
    cancel_requested: bool,
) -> TerminalKind:
    """判定表（冻结区 2.5 #24/#25），纯函数零 IO——M1.13 对抗测试可枚举穷举的前提。"""
    if cancel_requested:
        return TerminalKind.CANCELLED  # 取消压倒一切：产物一律丢弃走 T7（03 §2.1）
    match outcome:
        case OutcomeSuccess() | OutcomeDegraded():
            return TerminalKind.DONE
        case OutcomeFailure() as failure:
            if failure.replan_signal is not None:
                return TerminalKind.NEEDS_REPLAN  # T6a：信号压倒重试
            if failure.retryable and attempt < max_attempts:
                return TerminalKind.RETRY  # T3b
            if failure.retryable and replan_on_failure:
                return TerminalKind.NEEDS_REPLAN  # T6b：重试耗尽转重规划
            return TerminalKind.FAILED  # T5
    raise AssertionError(f"unhandled outcome: {outcome!r}")


# WHERE 守卫片段：WorkerGuard 逐字 G.3 三条件；ReaperGuard 为前置状态守卫（M1.11 分流②③）
_WORKER_WHERE = (
    "id = :node_id AND lease_owner = :worker_id AND attempt = :attempt AND status = 'RUNNING'"
)
_REAPER_WHERE = "id = :node_id AND status = 'RUNNING' AND lease_expires_at < now()"

_RETRY_SQL = {  # T3b：非终态——不动 attempt（Z-11）、不动 ready_at、不写 finished_at
    # 追加 cancel_requested_at IS NULL（C-7 族④）：带取消意图的行绝不回 READY——
    # G.1 跳过带意图的 READY、reaper 只巡 RUNNING，回去就是永久死区；
    # 意图压倒重试（判定表口径），拒绝后由调用方兜底走 T7
    WorkerGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'READY', lease_owner = NULL, lease_expires_at = NULL,
            error = CAST(:error AS jsonb)
        WHERE {_WORKER_WHERE} AND cancel_requested_at IS NULL
        """
    ),
    ReaperGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'READY', lease_owner = NULL, lease_expires_at = NULL,
            error = CAST(:error AS jsonb)
        WHERE {_REAPER_WHERE} AND cancel_requested_at IS NULL
        """
    ),
}

_FAILED_SQL = {  # T5：终态
    WorkerGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'FAILED', failure_class = :failure_class, error = CAST(:error AS jsonb),
            lease_owner = NULL, lease_expires_at = NULL, finished_at = now()
        WHERE {_WORKER_WHERE}
        """
    ),
    ReaperGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'FAILED', failure_class = :failure_class, error = CAST(:error AS jsonb),
            lease_owner = NULL, lease_expires_at = NULL, finished_at = now()
        WHERE {_REAPER_WHERE}
        """
    ),
}

_NEEDS_REPLAN_SQL = {  # T6：滞留态——不写 finished_at
    WorkerGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'NEEDS_REPLAN', failure_class = :failure_class,
            error = CAST(:error AS jsonb), lease_owner = NULL, lease_expires_at = NULL
        WHERE {_WORKER_WHERE}
        """
    ),
    ReaperGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'NEEDS_REPLAN', failure_class = :failure_class,
            error = CAST(:error AS jsonb), lease_owner = NULL, lease_expires_at = NULL
        WHERE {_REAPER_WHERE}
        """
    ),
}


def _guard_extra(guard: Guard) -> dict[str, Any]:
    if isinstance(guard, WorkerGuard):
        return {"worker_id": guard.worker_id, "attempt": guard.attempt}
    return {}


async def _attempt_of(session: AsyncSession, guard: Guard, node_id: NodeId) -> int:
    """挂点需要 attempt：worker 路径守卫自带；reaper 路径同事务补查。"""
    if isinstance(guard, WorkerGuard):
        return guard.attempt
    res = await session.execute(
        text("SELECT attempt FROM plan_nodes WHERE id = :n"), {"n": node_id}
    )
    return int(res.scalar_one())


@_retry_on_deadlock
async def commit_retry(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    node_id: NodeId,
    guard: Guard,
    error: dict[str, Any],
) -> bool:
    """03-T3b：可重试失败回 READY。非终态：无工件/信号/收尾判定，不锁任务行。"""
    async with session_factory() as session:
        res = await session.execute(
            _RETRY_SQL[type(guard)],
            {
                "node_id": node_id,
                "error": json.dumps(error, ensure_ascii=False),
                **_guard_extra(guard),
            },
        )
        if _rowcount(res) == 0:
            await session.rollback()
            return False
        await session.commit()
        return True


@_retry_on_deadlock
async def commit_failed(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    node_id: NodeId,
    guard: Guard,
    failure_class: FailureClass,
    error: dict[str, Any],
    signal: ReplanSignalDraft | None,
    hooks: BudgetHooks,
) -> bool:
    """03-T5 单事务：FAILED + 信号行（缺省 engine 自动生成）+ 挂点 + 任务收尾判定。"""
    async with session_factory() as session:
        await session.execute(_LOCK_TASK_SQL, {"task_id": task_id})
        res = await session.execute(
            _FAILED_SQL[type(guard)],
            {
                "node_id": node_id,
                "failure_class": failure_class.value,
                "error": json.dumps(error, ensure_ascii=False),
                **_guard_extra(guard),
            },
        )
        if _rowcount(res) == 0:
            await session.rollback()
            return False
        sig = (
            signal
            if signal is not None
            else ReplanSignalDraft(
                kind="node_replan", severity=2, payload={"cause": failure_class.value}
            )
        )
        await session.execute(
            _INSERT_SIGNAL_SQL,
            {
                "task_id": task_id,
                "node_id": node_id,
                "kind": sig.kind,
                "severity": sig.severity,
                "payload": json.dumps(sig.payload, ensure_ascii=False),
            },
        )
        await hooks.on_terminal(
            session,
            task_id=task_id,
            node_id=node_id,
            attempt=await _attempt_of(session, guard, node_id),
            terminal=NodeStatus.FAILED,
        )
        await _maybe_finalize_task(session, task_id)
        await session.commit()
    await _finalize_after_commit(session_factory, task_id)  # C-7 竞态补刀
    return True


@_retry_on_deadlock
async def commit_needs_replan(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    node_id: NodeId,
    guard: Guard,
    signal: ReplanSignalDraft,
    error: dict[str, Any] | None,
    failure_class: FailureClass | None,
    hooks: BudgetHooks,
) -> bool:
    """03-T6 单事务：滞留态（不写 finished_at）+ 信号行必写 + 挂点；不做任务收尾判定。"""
    async with session_factory() as session:
        await session.execute(_LOCK_TASK_SQL, {"task_id": task_id})
        res = await session.execute(
            _NEEDS_REPLAN_SQL[type(guard)],
            {
                "node_id": node_id,
                "failure_class": failure_class.value if failure_class is not None else None,
                "error": json.dumps(error, ensure_ascii=False) if error is not None else None,
                **_guard_extra(guard),
            },
        )
        if _rowcount(res) == 0:
            await session.rollback()
            return False
        await session.execute(
            _INSERT_SIGNAL_SQL,
            {
                "task_id": task_id,
                "node_id": node_id,
                "kind": signal.kind,
                "severity": signal.severity,
                "payload": json.dumps(signal.payload, ensure_ascii=False),
            },
        )
        await hooks.on_terminal(
            session,
            task_id=task_id,
            node_id=node_id,
            attempt=await _attempt_of(session, guard, node_id),
            terminal=NodeStatus.NEEDS_REPLAN,
        )
        await session.commit()
        return True


_CANCELLED_SQL = {  # T7：终态——取消产物一律丢弃（冻结区 2.5 #25），无工件无信号
    WorkerGuard: text(
        f"""
        UPDATE plan_nodes
        SET status = 'CANCELLED', lease_owner = NULL, lease_expires_at = NULL,
            finished_at = now()
        WHERE {_WORKER_WHERE}
        """
    ),
    ReaperGuard: text(  # 分支③守卫：过期 + 带取消意图（M1 册 M1.12 接口规格）
        """
        UPDATE plan_nodes
        SET status = 'CANCELLED', lease_owner = NULL, lease_expires_at = NULL,
            finished_at = now()
        WHERE id = :node_id AND status = 'RUNNING'
          AND lease_expires_at < now() AND cancel_requested_at IS NOT NULL
        """
    ),
}


@_retry_on_deadlock
async def commit_cancelled(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    node_id: NodeId,
    guard: Guard,
    hooks: BudgetHooks,
) -> bool:
    """03-T7 单事务：CANCELLED + 清租约 + 挂点（M2: SETTLE 含在途估计 + RELEASE）+ 收尾判定。"""
    async with session_factory() as session:
        await session.execute(_LOCK_TASK_SQL, {"task_id": task_id})
        res = await session.execute(
            _CANCELLED_SQL[type(guard)], {"node_id": node_id, **_guard_extra(guard)}
        )
        if _rowcount(res) == 0:
            await session.rollback()
            return False
        await hooks.on_terminal(
            session,
            task_id=task_id,
            node_id=node_id,
            attempt=await _attempt_of(session, guard, node_id),
            terminal=NodeStatus.CANCELLED,
        )
        await _maybe_finalize_task(session, task_id)
        await session.commit()
    await _finalize_after_commit(session_factory, task_id)  # C-7 竞态补刀
    return True
