"""EFFECTFUL 步骤日志与断点续跑（03 §4.3；M1 册 M1.9）。

纪律：每个方法一个独立小事务——步骤边界必须先于副作用持久化；复用外部长事务
就是设计违背（副作用发生了、意图行还没提交，崩溃后无从续跑）。
PURE 节点全程不碰本模块（03 §4.3 分野）。
"""

from typing import Any, NamedTuple, cast

from sqlalchemy import text
from sqlalchemy.engine import CursorResult, Result
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from argus.core.types import ArgusError, NodeId


class StepBegin(NamedTuple):
    skip: bool  # True = 该步骤已 DONE（上一 attempt 完成），直接跳过——零重做


class StepKeyMismatch(ArgusError):
    """同 (node, step_no) 的意图行 step_key 与本次不一致——步骤序错位，必须人查。"""


class StepStateError(ArgusError):
    """完成标记打不上：行不在 INTENT 态（重复 complete / 越过 begin）。"""


def _rowcount(res: Result[Any]) -> int:
    """同 lease._rowcount：收窄 AsyncSession.execute 的静态返回类型缝隙。"""
    return cast(CursorResult[Any], res).rowcount


_INSERT_INTENT_SQL = text(
    """
    INSERT INTO node_steps (node_id, attempt, step_no, step_key)
    VALUES (:node_id, :attempt, :step_no, :step_key)
    ON CONFLICT (node_id, step_no) DO NOTHING
    """
)

_READ_STEP_SQL = text(
    "SELECT step_key, status FROM node_steps WHERE node_id = :node_id AND step_no = :step_no"
)

_COMPLETE_SQL = text(
    """
    UPDATE node_steps SET status = 'DONE', result_digest = :digest
    WHERE node_id = :node_id AND step_no = :step_no AND status = 'INTENT'
    """
)

_EXISTS_SQL = text("SELECT EXISTS (SELECT 1 FROM node_steps WHERE node_id = :node_id)")

_LAST_DONE_SQL = text(
    "SELECT COALESCE(MAX(step_no), 0) FROM node_steps WHERE node_id = :node_id AND status = 'DONE'"
)


class StepJournal:
    """node_steps 访问层：先写意图、后干副作用、完成打标；续跑按 node 维度看完成集。"""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        node_id: NodeId,
        attempt: int,
    ) -> None:
        self._factory = session_factory
        self._node_id = node_id
        self._attempt = attempt

    async def exists(self) -> bool:
        """该节点有任何步骤行（worker 用它判断走 execute 还是 resume，M1.10）。"""
        async with self._factory() as session:
            res = await session.execute(_EXISTS_SQL, {"node_id": self._node_id})
            return bool(res.scalar_one())

    async def last_done(self) -> int:
        """最大已完成步骤号；无则 0（resume 的起点参数）。"""
        async with self._factory() as session:
            res = await session.execute(_LAST_DONE_SQL, {"node_id": self._node_id})
            return int(res.scalar_one())

    async def begin(self, step_no: int, step_key: str) -> StepBegin:
        """写意图行并提交（先落盘再执行）；回读分流三种现场（03 §4.3）。"""
        async with self._factory() as session:
            await session.execute(
                _INSERT_INTENT_SQL,
                {
                    "node_id": self._node_id,
                    "attempt": self._attempt,
                    "step_no": step_no,
                    "step_key": step_key,
                },
            )
            row = (
                await session.execute(
                    _READ_STEP_SQL, {"node_id": self._node_id, "step_no": step_no}
                )
            ).one()
            await session.commit()
        if row.status == "DONE":
            return StepBegin(skip=True)  # 上一 attempt 已完成：零重做
        if row.step_key != step_key:
            raise StepKeyMismatch(f"step {step_no}: journal has {row.step_key!r}, got {step_key!r}")
        return StepBegin(skip=False)  # 新步骤或接管半截步骤，均可执行

    async def complete(self, step_no: int, result_digest: str) -> None:
        """完成标记；WHERE status='INTENT' 守卫，打不上抛 StepStateError。"""
        async with self._factory() as session:
            res = await session.execute(
                _COMPLETE_SQL,
                {"node_id": self._node_id, "step_no": step_no, "digest": result_digest},
            )
            if _rowcount(res) == 0:
                await session.rollback()
                raise StepStateError(f"step {step_no} is not in INTENT state")
            await session.commit()
