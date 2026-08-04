"""入库 evidence_chunks（M0.9，[AI写]）：幂等 INSERT + 行数对账。

- 行主键 id = uuid5(NAMESPACE_URL, f"argus:{corpus_hash}:{chunk_id}")——确定性生成，
  重建库不漂移【实现细则】；
- chunk_id 落 locator JSONB（{"page", "char_offset", "chunk_id"}，02 未设实列的临时口径）；
- 幂等：ON CONFLICT (corpus_hash, text_hash) DO NOTHING；
- 收尾对账：库内行数 == corpus_meta.chunk_count，不等即报错退出。

CLI：uv run python -m scripts.corpus.load_pg --derived corpus/derived
     或 --fixture tests/fixtures/corpus（CI 夹具，同 schema 同算法）
"""

import argparse
import asyncio
import json
import sys
import uuid
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import text

from argus.core.config import ArgusSettings
from argus.core.db import build_engine

_INSERT = text(
    """
    INSERT INTO evidence_chunks
        (id, corpus_hash, source_id, doc_title, source_type, source_tier,
         published_at, locator, text, text_hash, embedding)
    VALUES
        (:id, :corpus_hash, :source_id, :doc_title, :source_type, :source_tier,
         :published_at, CAST(:locator AS jsonb), :text, :text_hash,
         CAST(:embedding AS vector))
    ON CONFLICT (corpus_hash, text_hash) DO NOTHING
    """
)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


async def load(derived: Path, dsn: str | None = None) -> tuple[int, int]:
    """返回 (目标行数, 入库后实测行数)。"""
    meta = json.loads((derived / "corpus_meta.json").read_text(encoding="utf-8"))
    corpus_hash = str(meta["corpus_hash"])
    chunks = _read_jsonl(derived / "chunks.jsonl")
    vectors = {str(r["chunk_id"]): r["vector"] for r in _read_jsonl(derived / "embeddings.jsonl")}

    if (
        dsn
    ):  # 测试注入 DSN 时不读 .env（_env_file 不在 dataclass_transform 合成签名里，见测试同款注释）
        settings = ArgusSettings(_env_file=None, pg_dsn=dsn)  # type: ignore[call-arg]
    else:
        settings = ArgusSettings()
    engine = build_engine(settings)
    try:
        batch: list[dict[str, Any]] = []
        async with engine.begin() as conn:
            for row in chunks:
                chunk_id = str(row["chunk_id"])
                vec = vectors[chunk_id]
                batch.append(
                    {
                        "id": uuid.uuid5(uuid.NAMESPACE_URL, f"argus:{corpus_hash}:{chunk_id}"),
                        "corpus_hash": corpus_hash,
                        "source_id": row["source_id"],
                        "doc_title": row["doc_title"],
                        "source_type": row["source_type"],
                        "source_tier": row["source_tier"],
                        "published_at": (
                            date.fromisoformat(str(row["published_at"]))
                            if row.get("published_at")
                            else None
                        ),
                        "locator": json.dumps(
                            {
                                "page": row["page"],
                                "char_offset": row["char_offset"],
                                "chunk_id": chunk_id,
                            }
                        ),
                        "text": row["text"],
                        "text_hash": row["text_hash"],
                        "embedding": "[" + ",".join(str(x) for x in vec) + "]",
                    }
                )
                if len(batch) >= 200:
                    await conn.execute(_INSERT, batch)
                    batch = []
            if batch:
                await conn.execute(_INSERT, batch)
            count = (
                await conn.execute(
                    text("SELECT count(*) FROM evidence_chunks WHERE corpus_hash = :h"),
                    {"h": corpus_hash},
                )
            ).scalar_one()
    finally:
        await engine.dispose()
    return len(chunks), int(count)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="derived/夹具 → evidence_chunks（幂等）")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--derived", type=Path)
    group.add_argument("--fixture", type=Path)
    args = parser.parse_args(argv)
    derived = args.derived or args.fixture
    meta = json.loads((derived / "corpus_meta.json").read_text(encoding="utf-8"))
    # 对账口径 = 去重后行数（unique_text_count）：UNIQUE (corpus_hash, text_hash)
    # 会合并语料内逐字相同的模板段；旧 meta 无此字段时回退 chunk_count
    expected = int(meta.get("unique_text_count", meta["chunk_count"]))
    chunk_total, actual = asyncio.run(load(derived))
    if expected != actual:
        print(f"行数对账失败：预期 {expected}（chunk 总数 {chunk_total}）≠ 实测 {actual}")
        return 1
    print(f"行数对账 OK：{actual} = {expected}（chunk 总数 {chunk_total}，库内去重后一致）")
    return 0


if __name__ == "__main__":
    sys.exit(main())
