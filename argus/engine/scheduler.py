"""租约调度器：领取事务（03 §3.1 / 速查表 G.1 逐字；M1 册 M1.6）。

事务形状：G.1 上半段（挑选+上锁+迁移，一条语句）→ started_at 补写（冻结区 2.5 #12）
→ 同事务补查 → 预算挂点（M2 在 on_claim 落 RESERVE/池扣减，M1 空转）→ COMMIT。
领取事务毫秒级提交是 ADR-004 红线：事务里绝不执行节点、绝不发网络调用。
attempt 只在这里 +1（Z-11：reaper 收回不加、重试回队不加）。
"""

from datetime import timedelta

from pydantic import BaseModel, ConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.types import NodeId, RoleName, TaskId
from argus.engine.graph import Purity
from argus.engine.ports import BudgetHooks
from argus.engine.store import NodeSpec


class ClaimedNode(BaseModel):
    """G.1 RETURNING + 同事务补查的合并视图（worker 据此构造 NodeContext）。"""

    model_config = ConfigDict(frozen=True)

    node_id: NodeId
    attempt: int
    role: RoleName
    purity: Purity
    plan_version_added: int
    task_id: TaskId
    spec: NodeSpec
    max_attempts: int


def compute_batch(c: int, in_flight: int) -> int:
    """batch = clamp(C − 在跑数, 0, C)（03 §3.5；冻结区 2.5 #15，C=8 为 Z-15 临时口径）。"""
    return max(0, min(c - in_flight, c))


# G.1 上半段【逐字】（03 §3.1）；下半段（RESERVE 流水+池扣减）是 M2 的 on_claim 挂点内容
_CLAIM_SQL = text(
    """
    WITH picked AS (
        SELECT id
        FROM plan_nodes
        WHERE status = 'READY'
          AND blocked_reason IS NULL
          AND cancel_requested_at IS NULL
          AND task_id IN (SELECT id FROM research_tasks
                          WHERE status IN ('PLANNING','EXECUTING')  -- 任务状态门
                          FOR SHARE SKIP LOCKED)  -- 与图手术的 FOR UPDATE 互斥（03 §5.3）
        ORDER BY priority DESC, ready_at
        LIMIT :batch
        FOR UPDATE SKIP LOCKED            -- 锁不到的行直接跳过，不等待
    )
    UPDATE plan_nodes n
    SET status = 'RUNNING',
        lease_owner = :worker_id,
        lease_expires_at = now() + :lease_ttl,
        attempt = n.attempt + 1                  -- attempt 只在这里 +1（Z-11）
    FROM picked
    WHERE n.id = picked.id
    RETURNING n.id, n.attempt, n.role, n.purity, n.plan_version_added
    """
)

_STARTED_AT_SQL = text(
    "UPDATE plan_nodes SET started_at = COALESCE(started_at, now()) WHERE id = ANY(:ids)"
)

_ENRICH_SQL = text("SELECT id, task_id, spec, max_attempts FROM plan_nodes WHERE id = ANY(:ids)")


async def claim_batch(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    worker_id: str,
    batch: int,
    lease_ttl_seconds: int,
    hooks: BudgetHooks,
) -> list[ClaimedNode]:
    """一个事务（显式 READ COMMITTED，决定 #16）领取至多 batch 个 READY 节点。"""
    if batch <= 0:
        return []
    async with session_factory() as session:
        # SET TRANSACTION 必须是事务第一条语句（session 首次 execute 即 autobegin）
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL READ COMMITTED"))
        res = await session.execute(
            _CLAIM_SQL,
            {
                "batch": batch,
                "worker_id": worker_id,
                "lease_ttl": timedelta(seconds=lease_ttl_seconds),
            },
        )
        picked = res.all()
        if not picked:
            await session.commit()
            return []
        ids = [row.id for row in picked]
        await session.execute(_STARTED_AT_SQL, {"ids": ids})
        enrich = await session.execute(_ENRICH_SQL, {"ids": ids})
        extra = {row.id: row for row in enrich}
        claimed: list[ClaimedNode] = []
        for row in picked:
            info = extra[row.id]
            node = ClaimedNode(
                node_id=NodeId(row.id),
                attempt=row.attempt,
                role=RoleName(row.role),
                purity=Purity(row.purity),
                plan_version_added=row.plan_version_added,
                task_id=TaskId(info.task_id),
                spec=NodeSpec.model_validate(info.spec),
                max_attempts=info.max_attempts,
            )
            claimed.append(node)
            # G.1 下半段挂点：M2 budget.py 在此写 RESERVE + 池扣减（同一连接同一事务）
            await hooks.on_claim(
                session, task_id=node.task_id, node_id=node.node_id, attempt=node.attempt
            )
        await session.commit()
        return claimed
