# Argus 语料库 · 合规与解析质量记录（M0.8 骨架，M0.9 定稿）

> 语料是**固定资产不是流水线**（08 六.5）：`corpus_hash` 封版后不改；
> 解析质量问题记录在本文件、人工修补后重新打哈希升版本；一切评测数字只出自封版语料。

## 一、目录约定

- `corpus/raw/<公司代号>/<source_id>.pdf` —— 原始归档（**不入库**）。文件名 = source_id
  （规避 Windows 260 字符路径限制；原始文件名与来源记 manifest）
- `corpus/derived/` —— `parsed/`、`chunks.jsonl`、`embeddings.jsonl`、`corpus_meta.json`（**不入库**）
- `corpus/manifest.json` —— 来源清单（**入库**，合规与复现凭证）

## 二、manifest 条目 schema（ADR-008 §2 字段）

```json
{
  "source_id": "yunshan-ar-2025",
  "doc_title": "某公司 2025 年年度报告",
  "source_type": "annual_report",
  "url": "https://static.cninfo.com.cn/...",
  "source_tier": 1,
  "fetched_at": "2026-08-05",
  "published_at": "2026-04-28",
  "sha256": "<文件 sha256：PowerShell (Get-FileHash 文件).Hash.ToLower()>",
  "license_note": "法定公开披露文件，快照仅演示用途",
  "parse_status": "pending"
}
```

- `source_type`：`annual_report` | `official_notice` | `news` | `judgment`（枚举冻结，速查表 D.1 #13）
- `source_tier`：1=年报 · 2=官方公示/司法文书 · 3=主流媒体（D.1 #12）
- `parse_status`：`pending` | `parsed` | `patched`（人工维护，解析结果以 parse_report.json 为准）

## 三、构建流水线（复现命令序列）

```powershell
uv run python -m scripts.corpus.parse_pdfs --raw corpus/raw --manifest corpus/manifest.json --out corpus/derived/parsed
uv run python -m scripts.corpus.chunk --parsed corpus/derived/parsed --manifest corpus/manifest.json --out corpus/derived/chunks.jsonl
# M0.9 接续：embed（费用上限内跑批）→ finalize（corpus_hash 封版）→ load_pg（入库）
```

## 四、解析质量记录（M0.9 定稿；逐份记录，无"默默丢弃"）

| source_id | 总页 | 失败页号 | 回退页数 | 修补页号 | 备注 |
|---|---|---|---|---|---|
| （待填：数据来自 `corpus/derived/parsed/parse_report.json`） | | | | | |

### 人工修补通道（"修补合法，痕迹留档"，ADR-008 §3）

解析失败或质量差的页：新建 `corpus/derived/parsed/<source_id>.patch.jsonl`，
每行 `{"page": 页号, "text": "人工校对后的文本"}`；重跑 parse 自动应用并在报告标记 `patched_pages`。

## 五、脱敏与人工抽查记录（07 §10.4）

- 自动清洗：身份证（18/15 位）/ 手机 / 固话 / 邮箱 → `［已脱敏］`，命中统计见 parse_report；
- 住址行**只标记不自动改**：`address_flag_lines` 是人工抽查清单，抽查结论（待填，M0.9）；
- 法人名称保留（研究对象就是公司）；自然人姓名化名在演示层处理，语料层不改；
- 不对裁判文书网已匿名化的内容做任何逆向尝试。

## 六、免责声明（占位，M0.9 定稿）

本语料为公开披露材料的版本化快照，仅用于技术演示与学习；系统输出不构成投资建议。
