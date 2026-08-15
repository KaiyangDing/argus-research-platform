"""租约维护与成功收尾：G.2 心跳 + G.3 fencing 完成事务（03 §3.3/§2.2；M1 册 M1.7）。

心跳按 G.2 原文（无 attempt 条件——冲突 C-1 按 03 施工，提交守卫兜底）。
终态事务锁序统一：任务行 FOR SHARE → 节点行守卫 UPDATE（G.3 注；与手术 FOR UPDATE 互斥）。
失败路径 M1.8 追加；取消路径 M1.12 追加。
"""

import json
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NamedTuple, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.types import ArgusError, NodeId, TaskId
from argus.engine.graph import NodeStatus
from argus.engine.ports import ArtifactDraft, BudgetHooks, ReplanSignalDraft


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
    """
    UPDATE plan_nodes SET status = 'READY', ready_at = now()
    WHERE status = 'PENDING'
      AND id IN (SELECT e.to_node FROM plan_edges e WHERE e.from_node = :done_node)
      AND NOT EXISTS (
          SELECT 1 FROM plan_edges e2
          JOIN plan_nodes p ON p.id = e2.from_node
          WHERE e2.to_node = plan_nodes.id AND p.status <> 'DONE')
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
        return True
