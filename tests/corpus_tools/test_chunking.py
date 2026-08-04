"""M0.8 确定性切块测试（施工手册 M0.8 测试清单）。合成文本，不碰真实语料。"""

import hashlib
import json

from scripts.corpus.chunk import build_rows, chunk_text


def _rows_for(pages: list[str], doc_sha: str) -> list[dict[str, object]]:
    return build_rows(
        pages,
        doc_sha=doc_sha,
        source_id="doc-1",
        doc_title="合成文档",
        source_type="annual_report",
        source_tier=1,
        published_at=None,
    )


def test_chunk_id_formula_matches_adr008() -> None:
    doc_sha = "f" * 64
    rows = _rows_for(["第一页正文内容。"], doc_sha)
    expected = hashlib.sha256(f"{doc_sha}:0".encode()).hexdigest()  # 手算 ADR-008 §2 公式
    assert rows[0]["chunk_id"] == expected
    assert rows[0]["char_offset"] == 0
    assert rows[0]["page"] == 1
    text = str(rows[0]["text"])
    assert rows[0]["text_hash"] == hashlib.sha256(text.encode()).hexdigest()


def test_chunk_determinism_same_input() -> None:
    page_a = ("云杉电商年度报告摘要。\n\n" * 30) + (
        "供应商合作风险章节，内容较长。" * 40 + "\n\n"
    ) * 3
    page_b = "诉讼与合规披露：" + "案件进展描述。" * 120
    pages = [page_a, page_b]
    one = json.dumps(_rows_for(pages, "a" * 64), ensure_ascii=False, sort_keys=True)
    two = json.dumps(_rows_for(pages, "a" * 64), ensure_ascii=False, sort_keys=True)
    assert one == two  # 同输入字节级一致（ADR-008 §2 确定性构建）


def test_long_paragraph_split_stable() -> None:
    text = "甲" * 3000  # 无段落分隔、无句界的最坏情况
    chunks = chunk_text(text)
    assert all(len(c) <= 1200 for _, c in chunks)
    assert "".join(c for _, c in chunks) == text  # 无缝隙无重叠：拼接即原文
    pos = 0
    for off, c in chunks:
        assert off == pos  # 偏移单调连续
        pos += len(c)


def test_short_paragraphs_merged() -> None:
    para = "短段落内容五十字左右" * 5 + "。\n"  # 约 52 字符的小段
    text = para * 30
    chunks = chunk_text(text)
    assert len(chunks) > 1
    for _, c in chunks[:-1]:
        assert len(c) >= 200  # 小段被合并至阈值以上（末块除外）
