"""corpus_hash 封版（M0.9，[AI写]）。

算法【实现细则】：corpus_hash = "sha256:" + sha256( sha256(manifest.json) + "\\n"
+ sha256(chunks.jsonl) + "\\n" + sha256(embeddings.jsonl) )——三个文件字节哈希按固定顺序
拼接再哈希；文件内容均为确定性构建产物，故"重新构建两次哈希一致"可验（05 §3.4 验收项 3）。
封版语义（08 六.5）：此后 derived 任何文件改动 = 新版本 = 重跑本脚本得新 hash。

CLI：uv run python -m scripts.corpus.finalize --derived corpus/derived --manifest corpus/manifest.json
"""

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path


def _file_sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compute_corpus_hash(manifest: Path, chunks: Path, embeddings: Path) -> str:
    combined = "\n".join([_file_sha(manifest), _file_sha(chunks), _file_sha(embeddings)])
    return "sha256:" + hashlib.sha256(combined.encode()).hexdigest()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="封版：计算 corpus_hash 并写 corpus_meta.json")
    parser.add_argument("--derived", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args(argv)

    chunks_path = args.derived / "chunks.jsonl"
    embeddings_path = args.derived / "embeddings.jsonl"
    chunk_ids = [
        str(json.loads(line)["chunk_id"])
        for line in chunks_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    embed_ids = [
        str(json.loads(line)["chunk_id"])
        for line in embeddings_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if sorted(chunk_ids) != sorted(embed_ids) or len(embed_ids) != len(set(embed_ids)):
        missing = set(chunk_ids) - set(embed_ids)
        extra = set(embed_ids) - set(chunk_ids)
        print(f"chunk↔embedding 不一一对应：缺 {len(missing)} / 多 {len(extra)}（先补跑 embed）")
        return 1

    unique_text_count = len(
        {
            str(json.loads(line)["text_hash"])
            for line in chunks_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
    )
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    corpus_hash = compute_corpus_hash(args.manifest, chunks_path, embeddings_path)
    meta = {
        "corpus_hash": corpus_hash,
        "doc_count": len(manifest["docs"]),
        "chunk_count": len(chunk_ids),
        # 库内行数口径：UNIQUE (corpus_hash, text_hash) 会把语料内逐字重复的模板段去重
        # （年报跨年度模板文本），入库对账以本值为准
        "unique_text_count": unique_text_count,
        "built_at": datetime.now(UTC).isoformat(),  # 仅记录，不参与哈希（meta 不入哈希三文件）
    }
    (args.derived / "corpus_meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(f"corpus_hash = {corpus_hash}")
    print(
        f"doc_count = {meta['doc_count']}, chunk_count = {meta['chunk_count']}, "
        f"unique_text_count = {unique_text_count}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
