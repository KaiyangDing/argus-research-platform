"""M1.2 图核心四表 schema 断言（M1 册测试清单，+9；DDL 唯一事实源 = 02 §3.2 / 速查表 B.1）。

全部用例在事务内造数后回滚，库保持干净、可重复执行（沿 M0 schema 测试口径）。
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

pytestmark = pytest.mark.pg

_TASK_COLUMNS = {
    "id",
    "title",
    "objective",
    "status",
    "plan_version",
    "corpus_hash",
    "budget_tokens_cap",
    "budget_yuan_cap",
    "idempotency_key",
    "requested_by",
    "created_at",
    "finished_at",
}

_NODE_COLUMNS = {
    "id",
    "task_id",
    "plan_version_added",
    "node_type",
    "role",
    "spec",
    "status",
    "attempt",
    "max_attempts",
    "purity",
    "priority",
    "ready_at",
    "blocked_reason",
    "cancel_requested_at",
    "cancel_reason",
    "lease_owner",
    "lease_expires_at",
    "checkpoint_artifact_id",
    "failure_class",
    "error",
    "created_at",
    "started_at",
    "finished_at",
}

_ARTIFACT_COLUMNS = {
    "id",
    "task_id",
    "producer_node",
    "kind",
    "schema_name",
    "schema_version",
    "payload",
    "headline",
    "digest",
    "refs",
    "topic_keys",
    "supersedes",
    "partial",
    "token_count",
    "content_hash",
    "created_at",
}

_TASK_INSERT = text(
    """
    INSERT INTO research_tasks
        (title, objective, status, corpus_hash, budget_tokens_cap, budget_yuan_cap, requested_by)
    VALUES (:title, :obj, :status, :ch, :tok, :yuan, :req)
    RETURNING id
    """
)

_NODE_INSERT = text(
    """
    INSERT INTO plan_nodes (task_id, plan_version_added, node_type, role, spec, status)
    VALUES (:task, 0, 'research', 'researcher', CAST(:spec AS jsonb), :status)
    RETURNING id
    """
)

_ARTIFACT_INSERT = text(
    """
    INSERT INTO artifacts (task_id, kind, schema_name, payload, headline, content_hash)
    VALUES (:task, :kind, 'research_memo', CAST(:payload AS jsonb), :headline, :hash)
    RETURNING id
    """
)


async def _mk_task(conn: AsyncConnection, status: str = "EXECUTING") -> object:
    res = await conn.execute(
        _TASK_INSERT,
        {
            "title": "t",
            "obj": "o",
            "status": status,
            "ch": "sha256:test",
            "tok": 1000,
            "yuan": "10.00",
            "req": "tester",
        },
    )
    return res.scalar_one()


async def _mk_node(conn: AsyncConnection, task_id: object, status: str = "PENDING") -> object:
    res = await conn.execute(_NODE_INSERT, {"task": task_id, "spec": "{}", "status": status})
    return res.scalar_one()


async def _columns(conn: AsyncConnection, table: str) -> dict[str, str]:
    res = await conn.execute(
        text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = :t"),
        {"t": table},
    )
    return {r[0]: r[1] for r in res}


async def test_research_tasks_columns_and_status_check(
    pg_engine: AsyncEngine, migrated: None
) -> None:
    async with pg_engine.connect() as conn:
        cols = await _columns(conn, "research_tasks")
        assert set(cols) == _TASK_COLUMNS  # 12 列，不多不少
        assert cols["budget_tokens_cap"] == "bigint"
        assert cols["budget_yuan_cap"] == "numeric"
        assert cols["created_at"] == "timestamp with time zone"
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        for status in [
            "SUBMITTED",
            "PLANNING",
            "AWAITING_APPROVAL",
            "EXECUTING",
            "DONE",
            "DONE_DEGRADED",
            "FAILED",
            "CANCELLED",
        ]:
            await _mk_task(conn, status=status)  # 八值逐字全放行
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        with pytest.raises(IntegrityError):
            await _mk_task(conn, status="RUNNING")  # 节点状态词不许混进任务状态机
        await tx.rollback()


async def test_plan_nodes_columns_union(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        cols = await _columns(conn, "plan_nodes")
        # 02 DDL ∪ Z-4 六增补列（速查表 B.1 已回填并集），23 列不多不少
        assert set(cols) == _NODE_COLUMNS
        assert "last_error" not in cols  # Z-4：错误现场列名以 02 的 error 为准
        assert cols["error"] == "jsonb"
        assert cols["spec"] == "jsonb"
        assert cols["cancel_requested_at"] == "timestamp with time zone"


async def test_plan_nodes_status_check_seven_values(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        for status in [
            "PENDING",
            "READY",
            "RUNNING",
            "DONE",
            "FAILED",
            "CANCELLED",
            "NEEDS_REPLAN",
        ]:
            await _mk_node(conn, task, status=status)
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        with pytest.raises(IntegrityError):
            await _mk_node(conn, task, status="CANCELLING")  # 取消不是状态（03 §2.1）
        await tx.rollback()


async def test_plan_nodes_purity_check(pg_engine: AsyncEngine, migrated: None) -> None:
    ins = text(
        """
        INSERT INTO plan_nodes (task_id, plan_version_added, node_type, role, spec, purity)
        VALUES (:task, 0, 'research', 'researcher', CAST('{}' AS jsonb), :purity)
        """
    )
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        node = await _mk_node(conn, task)  # 未显式给 purity
        res = await conn.execute(text("SELECT purity FROM plan_nodes WHERE id = :id"), {"id": node})
        assert res.scalar_one() == "pure"  # default 'pure'
        await conn.execute(ins, {"task": task, "purity": "effectful"})
        with pytest.raises(IntegrityError):
            await conn.execute(ins, {"task": task, "purity": "impure"})
        await tx.rollback()


async def test_partial_indexes_exist(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'plan_nodes'")
        )
        defs = {r[0]: r[1] for r in res}
        # 两条热路径部分索引：领取只扫 READY、reaper 只扫 RUNNING（02 §3.2）
        assert "WHERE" in defs["idx_nodes_ready"]
        assert "READY" in defs["idx_nodes_ready"]
        assert "WHERE" in defs["idx_nodes_lease"]
        assert "RUNNING" in defs["idx_nodes_lease"]


async def test_plan_edges_pk_and_no_self_loop(pg_engine: AsyncEngine, migrated: None) -> None:
    ins = text(
        """
        INSERT INTO plan_edges (task_id, from_node, to_node, plan_version_added)
        VALUES (:task, :src, :dst, 0)
        """
    )
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        a = await _mk_node(conn, task)
        b = await _mk_node(conn, task)
        await conn.execute(ins, {"task": task, "src": a, "dst": b})
        with pytest.raises(IntegrityError):
            await conn.execute(ins, {"task": task, "src": a, "dst": b})  # PK(from,to) 拒重复
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        a = await _mk_node(conn, task)
        with pytest.raises(IntegrityError):
            await conn.execute(ins, {"task": task, "src": a, "dst": a})  # CHECK 拒自环
        await tx.rollback()


async def test_artifacts_columns_and_kind_check(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        cols = await _columns(conn, "artifacts")
        assert set(cols) == _ARTIFACT_COLUMNS  # 16 列，不多不少
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        aid = None
        for i, kind in enumerate(
            [
                "plan_snapshot",
                "research_note",
                "analysis_table",
                "verification_verdict",
                "report_draft",
                "report_final",
            ]
        ):
            res = await conn.execute(
                _ARTIFACT_INSERT,
                {"task": task, "kind": kind, "payload": "{}", "headline": "h", "hash": f"s:{i}"},
            )
            aid = res.scalar_one()
        row = await conn.execute(
            text("SELECT refs, partial, schema_version FROM artifacts WHERE id = :id"),
            {"id": aid},
        )
        refs, partial, schema_version = row.one()
        assert refs == []  # refs 默认 '[]'
        assert partial is False  # partial 默认 false
        assert schema_version == 1
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        with pytest.raises(IntegrityError):
            await conn.execute(
                _ARTIFACT_INSERT,
                {"task": task, "kind": "summary", "payload": "{}", "headline": "h", "hash": "s"},
            )
        await tx.rollback()


async def test_checkpoint_fk_exists(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT confrelid::regclass::text FROM pg_constraint "
                "WHERE conname = 'fk_nodes_checkpoint'"
            )
        )
        # 迁移期后补的 FK（02 §3.2 注释）：checkpoint_artifact_id → artifacts(id)
        assert res.scalar_one() == "artifacts"


async def test_done_guard_trigger_rejects(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        node = await _mk_node(conn, task, status="DONE")
        with pytest.raises(DBAPIError):
            # I5：DONE 是终态，任何状态变更被触发器拒绝（03 §2.4）
            await conn.execute(
                text("UPDATE plan_nodes SET status = 'READY' WHERE id = :id"), {"id": node}
            )
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        task = await _mk_task(conn)
        node = await _mk_node(conn, task, status="DONE")
        res = await conn.execute(
            text("""UPDATE plan_nodes SET error = '{"note":"audit"}' WHERE id = :id"""),
            {"id": node},
        )
        assert res.rowcount == 1  # 非 status 列更新不受阻（触发器只挂 UPDATE OF status）
        await tx.rollback()
