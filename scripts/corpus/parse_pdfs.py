"""PDF 解析（M0.8，[AI写]）：原始 PDF → 页级文本（已脱敏）+ 解析报告。

纪律（05 §3.4 验收项 4 / ADR-008 §3）：
- 逐文档校验文件 sha256 == manifest 声明——manifest 是合规与复现凭证，不符即停；
- pdfplumber 逐页提取，空页/异常页回退 pypdf 再试；两者皆空记入失败清单，**绝不静默丢弃**；
- 人工修补通道：corpus/derived/parsed/<source_id>.patch.jsonl（行 = {page, text} 覆盖同页），
  二次运行时应用并在报告标记 patched_pages——"修补合法，痕迹留档"；
- 输出全页写入（失败页 text=""）保持页序完整，切块阶段的页映射依赖它。

CLI：uv run python -m scripts.corpus.parse_pdfs --raw corpus/raw
     --manifest corpus/manifest.json --out corpus/derived/parsed
"""

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import pdfplumber
import pypdf

from scripts.corpus.sanitize import SanitizeStats, flag_address_lines, sanitize_text


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _locate_pdf(raw_dir: Path, source_id: str) -> Path:
    matches = sorted(raw_dir.rglob(f"{source_id}.pdf"))
    if not matches:
        raise FileNotFoundError(
            f"corpus/raw 下找不到 {source_id}.pdf（命名约定见 corpus/README.md）"
        )
    if len(matches) > 1:
        raise FileNotFoundError(f"{source_id}.pdf 命中多个文件：{[str(m) for m in matches]}")
    return matches[0]


def _load_patch(patch_path: Path) -> dict[int, str]:
    if not patch_path.exists():
        return {}
    patch: dict[int, str] = {}
    for line in patch_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            row = json.loads(line)
            patch[int(row["page"])] = str(row["text"])
    return patch


def parse_document(pdf_path: Path, source_id: str, out_dir: Path) -> dict[str, Any]:
    """逐页提取 + 回退 + 修补 + 脱敏；返回该文档的报告条目。"""
    patch = _load_patch(out_dir / f"{source_id}.patch.jsonl")
    failed_pages: list[int] = []
    fallback_pages: list[int] = []
    patched_pages: list[int] = []
    address_flags: list[str] = []
    doc_stats = SanitizeStats()
    pages_out: list[dict[str, Any]] = []

    fallback_reader: Any | None = None
    with pdfplumber.open(pdf_path) as pdf:
        total_pages = len(pdf.pages)
        for page in pdf.pages:
            page_no = int(page.page_number)  # 1-based
            try:
                text = page.extract_text() or ""
            except Exception:  # noqa: BLE001  解析器异常形态繁杂，回退路径兜底
                text = ""
            if not text.strip():
                if fallback_reader is None:
                    fallback_reader = pypdf.PdfReader(str(pdf_path))
                try:
                    text = fallback_reader.pages[page_no - 1].extract_text() or ""
                except Exception:  # noqa: BLE001  两个解析器都放弃才算失败页
                    text = ""
                if text.strip():
                    fallback_pages.append(page_no)
            if page_no in patch:
                text = patch[page_no]
                patched_pages.append(page_no)
            if not text.strip() and page_no not in patch:
                failed_pages.append(page_no)
            clean, stats = sanitize_text(text)
            doc_stats.add(stats)
            address_flags.extend(flag_address_lines(clean))
            pages_out.append({"page": page_no, "text": clean})

    out_file = out_dir / f"{source_id}.jsonl"
    with out_file.open("w", encoding="utf-8", newline="\n") as f:
        for row in pages_out:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")

    return {
        "source_id": source_id,
        "total_pages": total_pages,
        "ok_pages": total_pages - len(failed_pages),
        "failed_pages": failed_pages,
        "fallback_pages": fallback_pages,
        "patched_pages": patched_pages,
        "sanitize_hits": {
            "id_number": doc_stats.id_number,
            "mobile": doc_stats.mobile,
            "landline": doc_stats.landline,
            "email": doc_stats.email,
        },
        "address_flag_lines": address_flags,  # 人工抽查清单（07 §10.4）
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="PDF → 页级文本 + 解析报告")
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args(argv)

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    docs = sorted(manifest["docs"], key=lambda d: str(d["source_id"]))
    if not docs:
        print("manifest.docs 为空：先完成语料归档并填写 manifest（见 corpus/README.md）")
        return 1

    args.out.mkdir(parents=True, exist_ok=True)
    report: dict[str, Any] = {"docs": []}
    for doc in docs:
        source_id = str(doc["source_id"])
        pdf_path = _locate_pdf(args.raw, source_id)
        actual_sha = _sha256_file(pdf_path)
        if actual_sha != doc["sha256"]:
            print(
                f"sha256 不符：{source_id}\n  manifest: {doc['sha256']}\n  实际:     {actual_sha}\n"
                "manifest 是合规与复现凭证，先修正归档或 manifest 再解析。"
            )
            return 1
        entry = parse_document(pdf_path, source_id, args.out)
        report["docs"].append(entry)
        print(
            f"{source_id}: {entry['ok_pages']}/{entry['total_pages']} 页成功"
            f"，失败 {entry['failed_pages'] or '无'}"
            f"，回退 {len(entry['fallback_pages'])} 页"
            f"，修补 {len(entry['patched_pages'])} 页"
            f"，脱敏命中 {sum(entry['sanitize_hits'].values())} 处"
        )

    report_path = args.out / "parse_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2), encoding="utf-8"
    )
    print(f"报告 → {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
