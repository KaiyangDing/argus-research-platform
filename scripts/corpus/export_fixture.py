"""CI 夹具语料包导出（M0.10，[AI写]）。

子集入选标准【实现细则】：仅法定公开披露文件（交易所公告）的整份小文档 + 合成文档
（虚构公司材料——ADR-008 备选 2 的合法用途：只服务功能测试，绝不进评测）；
新闻类（第三方版权内容）一律不入夹具（07 §10.3 合规红线）。

夹具与全量语料同 schema 同算法：独立跑 finalize 逻辑得**独立 corpus_hash**（07 §7.3）。
体积守卫【实现细则】：四文件合计 >8MB 即报错（07 §7.3"数 MB"；逼近上限的预案是
改走 GitHub Release 资产，此处仅留此注释不实现）。

CLI：uv run python -m scripts.corpus.export_fixture --derived corpus/derived
     --include corpus/fixture_include.json --out tests/fixtures/corpus
（合成文档缺 embedding 时需 ARGUS_ENV=demo、ARGUS_LLM_MODE=live 增量补齐，几厘钱级）
"""

import argparse
import asyncio
import hashlib
import json
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

from argus.core.config import ArgusSettings
from argus.core.llm import BudgetGate, CallContext, LLMClient
from scripts.corpus.chunk import build_rows
from scripts.corpus.finalize import compute_corpus_hash
from scripts.corpus.sanitize import sanitize_text

_MAIN_MANIFEST = Path("corpus/manifest.json")
_SIZE_CAP_BYTES = 8 * 1024 * 1024


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [
        json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
    ]


def _synthetic_rows(txt_path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """合成文档 → (manifest 条目, chunk 行)。走与真实语料同一条 sanitize→chunk 管线。"""
    raw = txt_path.read_text(encoding="utf-8")
    doc_sha = hashlib.sha256(txt_path.read_bytes()).hexdigest()
    clean, _stats = sanitize_text(raw)
    source_id = txt_path.stem
    doc_title = next(line for line in clean.splitlines() if line.strip())
    entry = {
        "source_id": source_id,
        "doc_title": doc_title,
        "source_type": "annual_report",
        "source_tier": 1,
        "url": f"synthetic://{txt_path.name}",
        "fetched_at": "2026-08-04",
        "published_at": "2026-01-01",
        "sha256": doc_sha,
        "license_note": "合成测试文档（虚构公司），仅功能测试、绝不进评测",
        "parse_status": "parsed",
    }
    rows = build_rows(
        [clean],
        doc_sha=doc_sha,
        source_id=source_id,
        doc_title=doc_title,
        source_type="annual_report",
        source_tier=1,
        published_at="2026-01-01",
    )
    return entry, rows


async def export(derived: Path, include_path: Path, out: Path) -> int:
    include = json.loads(include_path.read_text(encoding="utf-8"))
    main_manifest = json.loads(_MAIN_MANIFEST.read_text(encoding="utf-8"))
    docs_by_id = {d["source_id"]: d for d in main_manifest["docs"]}

    picked_ids = list(include["source_ids"])
    missing_docs = [sid for sid in picked_ids if sid not in docs_by_id]
    if missing_docs:
        print(f"include 里的 source_id 不在主 manifest：{missing_docs}")
        return 1

    all_chunks = _read_jsonl(derived / "chunks.jsonl")
    all_vectors = {
        str(r["chunk_id"]): r["vector"] for r in _read_jsonl(derived / "embeddings.jsonl")
    }
    real_rows = [r for r in all_chunks if r["source_id"] in set(picked_ids)]

    fixture_docs = [docs_by_id[sid] for sid in picked_ids]
    synthetic_rows: list[dict[str, Any]] = []
    for doc_path in include["synthetic_docs"]:
        entry, rows = _synthetic_rows(Path(doc_path))
        fixture_docs.append(entry)
        synthetic_rows.extend(rows)

    rows = sorted(
        real_rows + synthetic_rows, key=lambda r: (str(r["source_id"]), int(r["char_offset"]))
    )

    out.mkdir(parents=True, exist_ok=True)
    existing_vectors = {
        str(r["chunk_id"]): r["vector"] for r in _read_jsonl(out / "embeddings.jsonl")
    }
    vectors: dict[str, list[float]] = {}
    to_embed: list[dict[str, Any]] = []
    for r in rows:
        cid = str(r["chunk_id"])
        if cid in all_vectors:
            vectors[cid] = all_vectors[cid]  # 真实子集：直接复用全量 embedding（冻结资产不重算）
        elif cid in existing_vectors:
            vectors[cid] = existing_vectors[cid]  # 合成文档：上次导出已算过，断点复用
        else:
            to_embed.append(r)

    if to_embed:
        settings = ArgusSettings()
        if settings.llm_mode != "live":
            print(
                f"合成文档缺 {len(to_embed)} 条 embedding，需 ARGUS_ENV=demo、ARGUS_LLM_MODE=live 运行"
            )
            return 2
        client = LLMClient(settings, gates=[BudgetGate("fixture-embed", Decimal(1))])
        try:
            new_vecs = await client.embed(
                [str(r["text"]) for r in to_embed],
                CallContext(scenario_id="fixture-build", node_id="corpus"),
            )
        finally:
            await client.aclose()
        for r, v in zip(to_embed, new_vecs, strict=True):
            vectors[str(r["chunk_id"])] = v
        print(f"合成文档增量 embedding：{len(to_embed)} 条")

    manifest_path = out / "manifest.json"
    chunks_path = out / "chunks.jsonl"
    embeddings_path = out / "embeddings.jsonl"
    manifest_path.write_text(
        json.dumps({"docs": fixture_docs}, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    with chunks_path.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n")
    with embeddings_path.open("w", encoding="utf-8", newline="\n") as f:
        for r in rows:
            f.write(
                json.dumps({"chunk_id": r["chunk_id"], "vector": vectors[str(r["chunk_id"])]})
                + "\n"
            )

    fixture_hash = compute_corpus_hash(manifest_path, chunks_path, embeddings_path)
    meta = {
        "corpus_hash": fixture_hash,
        "doc_count": len(fixture_docs),
        "chunk_count": len(rows),
        "unique_text_count": len({str(r["text_hash"]) for r in rows}),
    }
    (out / "corpus_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    total_bytes = sum(
        p.stat().st_size
        for p in (manifest_path, chunks_path, embeddings_path, out / "corpus_meta.json")
    )
    if total_bytes > _SIZE_CAP_BYTES:
        print(f"夹具体积 {total_bytes / 1e6:.1f}MB 超过 8MB 守卫：减 include 或换合成文档")
        return 1
    print(
        f"夹具导出 OK：{meta['doc_count']} 文档 / {meta['chunk_count']} 块 / "
        f"{total_bytes / 1e6:.2f}MB\nfixture corpus_hash = {fixture_hash}"
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="封版语料 → CI 夹具包（独立 corpus_hash）")
    parser.add_argument("--derived", type=Path, required=True)
    parser.add_argument("--include", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)
    return asyncio.run(export(args.derived, args.include, args.out))


if __name__ == "__main__":
    sys.exit(main())
