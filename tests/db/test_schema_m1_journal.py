"""M1.3 node_steps / replan_signals schema 断言（M1 册测试清单，+4；DDL=本册实现细则，02 待补录）。

全部用例在事务内造数后回滚，库保持干净、可重复执行。
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

pytestmark = pytest.mark.pg

_STEP_INSERT = text(
    """
    INSERT INTO node_steps (node_id, attempt, step_no, step_key, status)
    VALUES (:node, 1, :no, :key, :status)
    """
)

_SIGNAL_INSERT = text(
    """
    INSERT INTO replan_signals (task_id, node_id, kind, severity, status)
    VALUES (:task, :node, :kind, 2, :status)
    RETURNING id
    """
)


async def _mk_task(conn: AsyncConnection) -> object:
    res = await conn.execute(
        text(
            """
            INSERT INTO research_tasks
                (title, objective, corpus_hash, budget_tokens_cap, budget_yuan_cap, requested_by)
            VALUES ('t', 'o', 'sha256:test', 1000, 10.00, 'tester')
            RETURNING id
            """
        )
    )
    return res.scalar_one()


async def _mk_node(conn: AsyncConnection, task_id: object) -> object:
    res = await conn.execute(
        text(
            """
            INSERT INTO plan_nodes (task_id, plan_version_added, node_type, role, spec)
            VALUES (:task, 0, 'research', 'researcher', CAST('{}' AS jsonb))
            RETURNING id
            """
        ),
        {"task": task_id},
    )
    return res.scalar_one()


async def test_node_steps_pk_and_unique_step_key(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        node = await _mk_node(conn, await _mk_task(conn))
        await conn.execute(_STEP_INSERT, {"node": node, "no": 1, "key": "s1", "status": "INTENT"})
        with pytest.raises(IntegrityError):
            # PK(node_id, step_no)：同节点同步骤号拒重复
            await conn.execute(
                _STEP_INSERT, {"node": node, "no": 1, "key": "s1b", "status": "INTENT"}
            )
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        node = await _mk_node(conn, await _mk_task(conn))
        await conn.execute(_STEP_INSERT, {"node": node, "no": 1, "key": "s1", "status": "INTENT"})
        with pytest.raises(IntegrityError):
            # UNIQUE(node_id, step_key)：幂等键同节点内唯一（03 §4.3）
            await conn.execute(
                _STEP_INSERT, {"node": node, "no": 2, "key": "s1", "status": "INTENT"}
            )
        await tx.rollback()


async def test_node_steps_status_check(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        node = await _mk_node(conn, await _mk_task(conn))
        await conn.execute(
            text(
                "INSERT INTO node_steps (node_id, attempt, step_no, step_key) VALUES (:n, 1, 1, 'a')"
            ),
            {"n": node},
        )
        res = await conn.execute(
            text("SELECT status FROM node_steps WHERE node_id = :n AND step_no = 1"), {"n": node}
        )
        assert res.scalar_one() == "INTENT"  # default 'INTENT'：先意图后执行
        await conn.execute(_STEP_INSERT, {"node": node, "no": 2, "key": "b", "status": "DONE"})
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        node = await _mk_node(conn, await _mk_task(conn))
        with pytest.raises(IntegrityError):
            await conn.execute(
                _STEP_INSERT, {"node": node, "no": 3, "key": "c", "status": "RUNNING"}
            )  # 两值之外拒收
        await tx.rollback()


async def test_replan_signals_kind_and_status_checks(
    pg_engine: AsyncEngine, migrated: None
) -> None:
    kinds = ["node_replan", "evidence_conflict", "coverage_gap", "verifier_negative"]
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        node = await _mk_node(conn, task)
        sid = None
        for kind in kinds:  # 四值逐字全放行（速查表 C.4）
            res = await conn.execute(
                _SIGNAL_INSERT, {"task": task, "node": node, "kind": kind, "status": "open"}
            )
            sid = res.scalar_one()
        for status in ["handled", "dismissed"]:
            await conn.execute(
                _SIGNAL_INSERT,
                {"task": task, "node": node, "kind": "node_replan", "status": status},
            )
        default_res = await conn.execute(
            text(
                "INSERT INTO replan_signals (task_id, kind, severity) "
                "VALUES (:task, 'node_replan', 2) RETURNING status"
            ),
            {"task": task},
        )
        assert default_res.scalar_one() == "open"  # default 'open'
        assert sid is not None
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        with pytest.raises(IntegrityError):
            await conn.execute(
                _SIGNAL_INSERT,
                {"task": task, "node": None, "kind": "node_retry", "status": "open"},
            )
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        with pytest.raises(IntegrityError):
            await conn.execute(
                _SIGNAL_INSERT,
                {"task": task, "node": None, "kind": "node_replan", "status": "closed"},
            )
        await tx.rollback()


async def test_replan_signals_open_partial_index(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT indexdef FROM pg_indexes "
                "WHERE tablename = 'replan_signals' AND indexname = 'idx_replan_signals_open'"
            )
        )
        indexdef = res.scalar_one()
        # M2 重规划循环只扫 open 信号（03 §5.1），部分索引与死信号规模脱钩
        assert "WHERE" in indexdef
        assert "open" in indexdef
