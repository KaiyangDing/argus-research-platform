"""确定性切块（M0.8，[AI写]）：ADR-008 chunk_id 公式落地。

切块参数【实现细则·可实测后调整】：段落边界（连续换行 或 全角句号+换行）聚合，
目标 800 字符、硬上限 1200、短段落随目标值自然合并（≥200，被上限挤压与末块除外）；
超长段落按句切、无句界按上限硬切；overlap = 0。
归一化只统一换行为 \\n、去 BOM，**不做 NFKC**——不改原文字符，char_offset 才可复现。
确定性：文档按 source_id 排序、块按 char_offset 排序，无随机无时间戳——同输入必得同输出
（ADR-008 §2"确定性构建"是 M0 验收项）。

CLI：uv run python -m scripts.corpus.chunk --parsed corpus/derived/parsed
     --manifest corpus/manifest.json --out corpus/derived/chunks.jsonl
"""

import argparse
import hashlib
import json
import sys
from bisect import bisect_right
from pathlib import Path
from typing import Any

TARGET_CHARS = 800
MAX_CHARS = 1200
MERGE_MIN_CHARS = 200
_SENTENCE_ENDS = "。！？"


def normalize(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n").replace(chr(0xFEFF), "")  # 去 BOM


def _split_paragraphs(text: str) -> list[tuple[int, str]]:
    """切在连续换行之后或"。\\n"之后，分隔符归前段——各段拼接可逐字节重建原文。"""
    parts: list[tuple[int, str]] = []
    start = 0
    i = 0
    n = len(text)
    while i < n:
        if text[i] == "\n" and i + 1 < n and text[i + 1] == "\n":
            j = i
            while j < n and text[j] == "\n":
                j += 1
            parts.append((start, text[start:j]))
            start = j
            i = j
        elif text[i] == "。" and i + 1 < n and text[i + 1] == "\n":
            parts.append((start, text[start : i + 2]))
            start = i + 2
            i += 2
        else:
            i += 1
    if start < n:
        parts.append((start, text[start:]))
    return parts


def _split_long(offset: int, text: str) -> list[tuple[int, str]]:
    """超长段落按句切；窗口内无句界则按上限硬切。偏移连续、无缝隙。"""
    units: list[tuple[int, str]] = []
    start = 0
    n = len(text)
    while n - start > MAX_CHARS:
        window = text[start : start + MAX_CHARS]
        cut_rel = max(window.rfind(ch) for ch in _SENTENCE_ENDS)
        cut = start + (cut_rel + 1 if cut_rel >= 0 else MAX_CHARS)
        units.append((offset + start, text[start:cut]))
        start = cut
    units.append((offset + start, text[start:]))
    return units


def chunk_text(text: str) -> list[tuple[int, str]]:
    """(char_offset, chunk) 列表；各块拼接 == 原文（overlap=0、无缝隙）。"""
    units: list[tuple[int, str]] = []
    for off, para in _split_paragraphs(text):
        if len(para) <= MAX_CHARS:
            units.append((off, para))
        else:
            units.extend(_split_long(off, para))
    chunks: list[tuple[int, str]] = []
    buf: list[str] = []
    buf_off = 0
    buf_len = 0
    for off, unit in units:
        if buf and buf_len + len(unit) > MAX_CHARS:
            chunks.append((buf_off, "".join(buf)))
            buf = []
            buf_len = 0
        if not buf:
            buf_off = off
        buf.append(unit)
        buf_len += len(unit)
        if buf_len >= TARGET_CHARS:
            chunks.append((buf_off, "".join(buf)))
            buf = []
            buf_len = 0
    if buf:
        chunks.append((buf_off, "".join(buf)))
    return chunks


def build_rows(
    pages: list[str],
    *,
    doc_sha: str,
    source_id: str,
    doc_title: str,
    source_type: str,
    source_tier: int,
    published_at: str | None,
) -> list[dict[str, Any]]:
    """页文本 → 归一化全文 → 切块 → 行 schema（含 chunk_id 公式与页映射）。"""
    normalized_pages = [normalize(p) for p in pages]
    full = "\n".join(normalized_pages)
    page_starts: list[int] = []
    pos = 0
    for p in normalized_pages:
        page_starts.append(pos)
        pos += len(p) + 1  # +1 是页间连接符 \n
    rows: list[dict[str, Any]] = []
    for offset, chunk in chunk_text(full):
        if not chunk.strip():  # 纯空白块（解析失败页产生）不进检索库；偏移体系不受影响
            continue
        rows.append(
            {
                # ADR-008 §2 公式：chunk_id = sha256(f"{doc_sha}:{char_offset}")【实现细则·编码】
                "chunk_id": hashlib.sha256(f"{doc_sha}:{offset}".encode()).hexdigest(),
                "doc_sha": doc_sha,
                "source_id": source_id,
                "doc_title": doc_title,
                "source_type": source_type,
                "source_tier": source_tier,
                "published_at": published_at,
                "page": bisect_right(page_starts, offset),  # page_starts 从 0 起 → 1-based 页号
                "char_offset": offset,
                "text": chunk,
                "text_hash": hashlib.sha256(chunk.encode()).hexdigest(),
            }
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="确定性切块：parsed jsonl → chunks.jsonl")
    parser.add_argument("--parsed", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    docs = sorted(manifest["docs"], key=lambda d: str(d["source_id"]))
    if not docs:
        print("manifest.docs 为空：先完成语料归档并填写 manifest（见 corpus/README.md）")
        return 1

    all_rows: list[dict[str, Any]] = []
    for doc in docs:
        source_id = doc["source_id"]
        parsed_file = args.parsed / f"{source_id}.jsonl"
        if not parsed_file.exists():
            print(f"缺解析产物：{parsed_file}（先跑 scripts.corpus.parse_pdfs）")
            return 1
        page_rows = [
            json.loads(line)
            for line in parsed_file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        page_rows.sort(key=lambda r: int(r["page"]))
        pages = [str(r["text"]) for r in page_rows]
        rows = build_rows(
            pages,
            doc_sha=doc["sha256"],
            source_id=source_id,
            doc_title=doc["doc_title"],
            source_type=doc["source_type"],
            source_tier=int(doc["source_tier"]),
            published_at=doc.get("published_at"),
        )
        all_rows.extend(rows)
        print(f"{source_id}: {len(pages)} 页 → {len(rows)} 块")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8", newline="\n") as f:
        for row in all_rows:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    print(f"共 {len(all_rows)} 块 → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
