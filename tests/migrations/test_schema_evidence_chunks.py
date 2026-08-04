"""M0.5 evidence_chunks schema 断言（施工手册 M0.5 测试清单；DDL 唯一事实源 = 02 §3.2）。

全部用例在事务内 INSERT 后回滚，库保持干净、可重复执行。
"""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.pg

_COLUMNS = {
    "id",
    "corpus_hash",
    "source_id",
    "doc_title",
    "source_type",
    "source_tier",
    "published_at",
    "locator",
    "text",
    "text_hash",
    "embedding",
    "tsv",
}

_INSERT = text(
    """
    INSERT INTO evidence_chunks
        (corpus_hash, source_id, doc_title, source_type, source_tier, locator, text, text_hash)
    VALUES (:ch, :sid, :title, :stype, :tier, CAST(:loc AS jsonb), :txt, :th)
    """
)


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "ch": "sha256:test",
        "sid": "doc-1",
        "title": "t",
        "stype": "annual_report",
        "tier": 1,
        "loc": "{}",
        "txt": "正文",
        "th": "h-0",
    }
    row.update(overrides)
    return row


async def test_table_and_columns_exact(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'evidence_chunks'"
            )
        )
        assert {r[0] for r in res} == _COLUMNS  # 12 列，不多不少


async def test_source_type_check_enum(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        with pytest.raises(IntegrityError):
            await conn.execute(_INSERT, _row(stype="blog"))
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        for i, stype in enumerate(["annual_report", "official_notice", "news", "judgment"]):
            await conn.execute(_INSERT, _row(stype=stype, th=f"h-{i}"))
        await tx.rollback()


async def test_unique_corpus_text_hash(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        await conn.execute(_INSERT, _row())
        with pytest.raises(IntegrityError):
            await conn.execute(_INSERT, _row())
        await tx.rollback()


async def test_embedding_dimension_1024(pg_engine: AsyncEngine, migrated: None) -> None:
    ins = text(
        """
        INSERT INTO evidence_chunks
            (corpus_hash, source_id, doc_title, source_type, source_tier,
             locator, text, text_hash, embedding)
        VALUES (:ch, :sid, :title, :stype, :tier,
                CAST(:loc AS jsonb), :txt, :th, CAST(:vec AS vector))
        """
    )
    ok_vec = "[" + ",".join(["0"] * 1024) + "]"
    bad_vec = "[" + ",".join(["0"] * 1023) + "]"
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        await conn.execute(ins, _row(th="h-dim-ok") | {"vec": ok_vec})
        await tx.rollback()
    async with pg_engine.connect() as conn:
        tx = await conn.begin()
        with pytest.raises(DBAPIError):
            await conn.execute(ins, _row(th="h-dim-bad") | {"vec": bad_vec})
        await tx.rollback()


async def test_indexes_present(pg_engine: AsyncEngine, migrated: None) -> None:
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'evidence_chunks'")
        )
        defs = {r[0]: r[1] for r in res}
        assert "idx_chunks_vec" in defs
        assert "hnsw" in defs["idx_chunks_vec"]
        assert "idx_chunks_tsv" in defs
        assert "gin" in defs["idx_chunks_tsv"].lower()
        gen = await conn.execute(
            text(
                "SELECT is_generated FROM information_schema.columns "
                "WHERE table_name = 'evidence_chunks' AND column_name = 'tsv'"
            )
        )
        assert gen.scalar_one() == "ALWAYS"  # GENERATED ALWAYS AS ... STORED
