"""M0.9 入库幂等测试（施工手册 M0.9 测试清单）。合成 3 块小语料，真打 PG。"""

import json
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from scripts.corpus.finalize import compute_corpus_hash
from scripts.corpus.load_pg import load

pytestmark = pytest.mark.pg


def _build_synthetic_corpus(root: Path) -> str:
    manifest = root / "manifest.json"
    chunks = root / "chunks.jsonl"
    embeddings = root / "embeddings.jsonl"
    manifest.write_text('{"docs": [{"source_id": "syn-1"}]}', encoding="utf-8")
    vec = [0.0] * 1024
    chunk_rows = []
    embed_rows = []
    for i in range(3):
        chunk_rows.append(
            {
                "chunk_id": f"c{i}",
                "doc_sha": "f" * 64,
                "source_id": "syn-1",
                "doc_title": "合成文档",
                "source_type": "annual_report",
                "source_tier": 1,
                "published_at": "2026-01-01",
                "page": 1,
                "char_offset": i * 100,
                "text": f"合成第{i}块",
                "text_hash": f"th-{i}",
            }
        )
        embed_rows.append({"chunk_id": f"c{i}", "vector": vec})
    chunks.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in chunk_rows), encoding="utf-8"
    )
    embeddings.write_text("".join(json.dumps(r) + "\n" for r in embed_rows), encoding="utf-8")
    corpus_hash = compute_corpus_hash(manifest, chunks, embeddings)
    (root / "corpus_meta.json").write_text(
        json.dumps(
            {
                "corpus_hash": corpus_hash,
                "doc_count": 1,
                "chunk_count": 3,
                "unique_text_count": 3,
            }
        ),
        encoding="utf-8",
    )
    return corpus_hash


async def test_load_twice_row_count_stable(
    pg_engine: AsyncEngine, migrated: None, pg_dsn: str, tmp_path: Path
) -> None:
    corpus_hash = _build_synthetic_corpus(tmp_path)
    async with pg_engine.begin() as conn:  # 清掉历史测试残留，保证从零可复验
        await conn.execute(
            text("DELETE FROM evidence_chunks WHERE corpus_hash = :h"), {"h": corpus_hash}
        )

    target1, actual1 = await load(tmp_path, dsn=pg_dsn)
    assert (target1, actual1) == (3, 3)

    async with pg_engine.connect() as conn:
        ids_first = [
            str(r[0])
            for r in await conn.execute(
                text("SELECT id FROM evidence_chunks WHERE corpus_hash = :h ORDER BY id"),
                {"h": corpus_hash},
            )
        ]

    target2, actual2 = await load(tmp_path, dsn=pg_dsn)  # 第二遍：幂等
    assert (target2, actual2) == (3, 3)

    async with pg_engine.connect() as conn:
        ids_second = [
            str(r[0])
            for r in await conn.execute(
                text("SELECT id FROM evidence_chunks WHERE corpus_hash = :h ORDER BY id"),
                {"h": corpus_hash},
            )
        ]
    assert ids_first == ids_second  # uuid5 确定性：两遍行 id 一致，重建库不漂移
