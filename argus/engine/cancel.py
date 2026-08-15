"""取消传播链（03 §7 / 速查表 G.7；M1 册 M1.12）。

取消意图不是状态、是列（03 §2.1）：①意图落库+即时 T8 由本模块负责；
②运行时收敛在 worker（心跳观察→token→PURE 硬杀/EFFECTFUL 步骤边界+T_grace）；
③T7 终态事务在 lease.commit_cancelled。三步序不可交换（G.7 顺序论证）。
"""

import asyncio
from collections.abc import Sequence

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.types import NodeId, TaskId
from argus.engine.lease import _finalize_after_commit, _maybe_finalize_task
from argus.engine.ports import BudgetHooks


class CancelToken:
    """asyncio.Event 的窄包装（【实现细则】）：执行体只见 set/is_set/wait 三件事。"""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def set(self) -> None:
        self._event.set()

    def is_set(self) -> bool:
        return self._event.is_set()

    async def wait(self) -> None:
        await self._event.wait()


_SUBTREE_SQL = text(  # 递归 CTE，含 root 自身（M1 册 M1.12 接口规格）
    """
    WITH RECURSIVE sub AS (
      SELECT CAST(:root AS uuid) AS id
      UNION
      SELECT e.to_node FROM plan_edges e JOIN sub s ON e.from_node = s.id)
    SELECT id FROM sub
    """
)

_INTENT_SQL = text(  # G.7①【逐字】+ 幂等追加条件（02 §2.3"取消幂等"）
    """
    UPDATE plan_nodes SET cancel_requested_at = now(), cancel_reason = :r
    WHERE id = ANY(:node_ids) AND status IN ('PENDING','READY','RUNNING')
      AND cancel_requested_at IS NULL
    """
)

_T8_SQL = text(  # 无在途工作：即时终态（03-T8）
    """
    UPDATE plan_nodes SET status = 'CANCELLED', finished_at = now()
    WHERE id = ANY(:node_ids) AND status IN ('PENDING','READY')
    RETURNING id
    """
)

_NONTERMINAL_SQL = text(
    """
    SELECT id FROM plan_nodes
    WHERE task_id = :task_id AND status NOT IN ('DONE','FAILED','CANCELLED')
    """
)

_LOCK_TASK_SQL = text("SELECT 1 FROM research_tasks WHERE id = :task_id FOR SHARE")


async def subtree_ids(session: AsyncSession, root: NodeId) -> list[NodeId]:
    """root 及其全部后代（不含兄弟）——取消完备性的覆盖面就是这个集合。"""
    res = await session.execute(_SUBTREE_SQL, {"root": root})
    return [NodeId(row.id) for row in res]


async def request_cancel(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    node_ids: Sequence[NodeId],
    reason: str,
    hooks: BudgetHooks,
) -> None:
    """G.7① 单事务：意图 + 即时 T8 + 剪枝挂点 + 任务收尾判定。

    RUNNING 行只落意图（②③由 worker/reaper 收敛）；幂等：已带意图的行不覆盖。
    """
    ids = list(node_ids)
    if not ids:
        return
    async with session_factory() as session:
        # 锁序统一：任务行 FOR SHARE 先行（T8 是终态迁移，与手术互斥）
        await session.execute(_LOCK_TASK_SQL, {"task_id": task_id})
        await session.execute(_INTENT_SQL, {"node_ids": ids, "r": reason})
        pruned = (await session.execute(_T8_SQL, {"node_ids": ids})).all()
        if pruned:
            # M2 在此 RELEASE 崩溃遗留的活跃 RESERVE（03-T8）；M1 空转
            await hooks.on_prune(session, node_ids=[NodeId(row.id) for row in pruned])
        await _maybe_finalize_task(session, task_id)
        await session.commit()
    await _finalize_after_commit(session_factory, task_id)  # C-7 竞态补刀（T8 亦可能是最后终态）


async def cancel_task(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    task_id: TaskId,
    reason: str,
    hooks: BudgetHooks,
) -> None:
    """任务级取消：全部非终态节点一把撤（复用 request_cancel）。"""
    async with session_factory() as session:
        res = await session.execute(_NONTERMINAL_SQL, {"task_id": task_id})
        ids = [NodeId(row.id) for row in res]
    await request_cancel(session_factory, task_id=task_id, node_ids=ids, reason=reason, hooks=hooks)
