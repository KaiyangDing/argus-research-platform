"""M0.9 corpus_hash 封版测试（施工手册 M0.9 测试清单）。tmp_path 合成小语料，不碰真实 derived。"""

from pathlib import Path

from scripts.corpus.finalize import compute_corpus_hash


def _write_corpus(root: Path) -> tuple[Path, Path, Path]:
    manifest = root / "manifest.json"
    chunks = root / "chunks.jsonl"
    embeddings = root / "embeddings.jsonl"
    manifest.write_text('{"docs": [{"source_id": "d1"}]}', encoding="utf-8")
    chunks.write_text('{"chunk_id": "c1", "text": "正文"}\n', encoding="utf-8")
    embeddings.write_text('{"chunk_id": "c1", "vector": [0.1]}\n', encoding="utf-8")
    return manifest, chunks, embeddings


def test_hash_deterministic_two_runs(tmp_path: Path) -> None:
    manifest, chunks, embeddings = _write_corpus(tmp_path)
    first = compute_corpus_hash(manifest, chunks, embeddings)
    second = compute_corpus_hash(manifest, chunks, embeddings)
    assert first == second
    assert first.startswith("sha256:")
    # built_at 只写 corpus_meta.json、不参与哈希：纯函数两跑同值即为证


def test_hash_changes_on_any_content_change(tmp_path: Path) -> None:
    manifest, chunks, embeddings = _write_corpus(tmp_path)
    before = compute_corpus_hash(manifest, chunks, embeddings)
    chunks.write_text('{"chunk_id": "c1", "text": "正文改"}\n', encoding="utf-8")
    after = compute_corpus_hash(manifest, chunks, embeddings)
    assert before != after  # 任何一个字符的变化都必须换新版本号（封版语义，08 六.5）
