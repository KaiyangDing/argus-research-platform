"""M0.10 夹具语料包检索冒烟（施工手册 M0.10 测试清单）。

前提：夹具已导入 PG（uv run python -m scripts.corpus.load_pg --fixture tests/fixtures/corpus）。
本地未导入时 skip 并提示导入命令；CI 因 REQUIRE_PG + ci.yml 的导入步必然真实执行。
"""

import json
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

pytestmark = pytest.mark.pg

_FIXTURE = Path("tests/fixtures/corpus")


def _meta() -> dict[str, object]:
    return json.loads((_FIXTURE / "corpus_meta.json").read_text(encoding="utf-8"))


async def _fixture_rows(conn_engine: AsyncEngine, corpus_hash: str) -> int:
    async with conn_engine.connect() as conn:
        res = await conn.execute(
            text("SELECT count(*) FROM evidence_chunks WHERE corpus_hash = :h"),
            {"h": corpus_hash},
        )
        return int(res.scalar_one())


async def _require_loaded(pg_engine: AsyncEngine, corpus_hash: str) -> int:
    count = await _fixture_rows(pg_engine, corpus_hash)
    if count == 0:
        pytest.skip(
            "夹具未导入：uv run python -m scripts.corpus.load_pg --fixture tests/fixtures/corpus"
        )
    return count


async def test_fixture_rows_match_meta(pg_engine: AsyncEngine, migrated: None) -> None:
    meta = _meta()
    count = await _require_loaded(pg_engine, str(meta["corpus_hash"]))
    expected = int(str(meta.get("unique_text_count", meta["chunk_count"])))
    assert count == expected


async def test_fixture_hash_differs_from_full_corpus(pg_engine: AsyncEngine, migrated: None) -> None:
    meta = _meta()
    fixture_hash = str(meta["corpus_hash"])
    assert fixture_hash.startswith("sha256:")
    full_meta_path = Path("corpus/derived/corpus_meta.json")
    if full_meta_path.exists():  # 口径隔离：夹具只服务功能测试，评测数字只出自全量语料
        full_hash = str(json.loads(full_meta_path.read_text(encoding="utf-8"))["corpus_hash"])
        assert fixture_hash != full_hash


async def test_tsv_search_hits(pg_engine: AsyncEngine, migrated: None) -> None:
    meta = _meta()
    corpus_hash = str(meta["corpus_hash"])
    await _require_loaded(pg_engine, corpus_hash)
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT count(*) FROM evidence_chunks "
                "WHERE corpus_hash = :h AND tsv @@ plainto_tsquery('simple', :q)"
            ),
            {"h": corpus_hash, "q": "qingshuang"},  # 合成文档预埋的空格分隔检索标记
        )
        assert int(res.scalar_one()) >= 1


async def test_vector_search_topk(pg_engine: AsyncEngine, migrated: None) -> None:
    meta = _meta()
    corpus_hash = str(meta["corpus_hash"])
    await _require_loaded(pg_engine, corpus_hash)
    first = json.loads(
        (_FIXTURE / "embeddings.jsonl").read_text(encoding="utf-8").splitlines()[0]
    )
    vec_text = "[" + ",".join(str(x) for x in first["vector"]) + "]"
    async with pg_engine.connect() as conn:
        res = await conn.execute(
            text(
                "SELECT locator->>'chunk_id' FROM evidence_chunks "
                "WHERE corpus_hash = :h "
                "ORDER BY embedding <=> CAST(:v AS vector) LIMIT 3"
            ),
            {"h": corpus_hash, "v": vec_text},
        )
        top = [row[0] for row in res]
    assert top[0] == str(first["chunk_id"])  # 自检索恒等：向量与 hnsw 索引通路全通
