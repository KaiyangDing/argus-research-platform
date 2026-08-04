# M0 语料清单与解析质量凭证（reports/m0_corpus_manifest.md）

> 2026-08-04 · 05 §15 登记凭证 · 起草 [AI写]，数字经用户核对
> **corpus_hash = `sha256:129d200e9a3592e74b6d4364c379d2cf59312b9313698a6e5369bd74d7e12150`**

## 一、公司清单（默认盘 3 家；stretch 未启用）

| 公司 | 代码 | 选司依据（硬性条件：近三年可公开获取的诉讼/合规披露） |
|---|---|---|
| 良品铺子 | 603719 | 2024-11 配料表举报事件：自媒体指控 / 市监局通报不成立 / 公司公告三方口径俱全 |
| 永辉超市 | 601933 | 连年亏损与闭店、名创优品入股权益变动、年报"重大诉讼"节 9 案披露 |
| 三只松鼠 | 300783 | 与良品铺子同业直接可比；2025 年 H 股 IPO 系列披露 |

备选池（未启用）：顺丰控股、圆通速递、韵达股份、歌尔股份、立讯精密、双汇发展、洽洽食品。

## 二、来源类型 × 家数矩阵（23 份，2141 页）

| source_type | tier | lpz | yh | szss | 合计 |
|---|---|---|---|---|---|
| annual_report（2023/2024/2025） | 1 | 3 | 3 | 3 | 9 |
| official_notice（交易所公告） | 2 | 1 | 3 | 3 | 7 |
| news（正文重渲染存档） | 3 | 2 | 3 | 2 | 7 |

诉讼与合规披露口径按 05 §3.5 回退阶梯执行（年报"重大诉讼仲裁"章节 + 事件公告；judgment 类未采集，文书网可得性受限，回退合规）。

## 三、构建结果与对账

| 项 | 数值 |
|---|---|
| chunk 总数 | 1995（186 万字符；块长中位 940，81% 在 800–1200 目标带，0 块超上限） |
| 库内去重后行数（unique_text_count） | **1991**（4 组跨年度年报模板段逐字重复，被 `UNIQUE (corpus_hash, text_hash)` 合并，明细见 corpus/README.md） |
| `evidence_chunks` 入库对账 | **OK：1991 = 1991**（load_pg 收尾自检） |
| 解析质量 | 22/23 份零失败；yh-notice-20240928 末页为签章图像页，判定不修补留档 |
| 确定性验证 | chunk 两次构建字节级一致；finalize 两次 corpus_hash 一致（05 §3.4 验收项 3 ✓） |

## 四、embedding 实际花费（口径：脚本按价目表计费；待用户与百炼账单/抵扣明细核对）

| 项 | 数值 |
|---|---|
| 实际 token 量 | ≈139.7 万（脚本逐批累计） |
| **实际花费** | **¥0.6983**（text-embedding-v4 @ ¥0.0005/千 token；节省计划抵扣范围内） |
| 预估偏差记录 | 事前报价 ¥0.46（"中文≈2字符/token"粗估）；实测偏高 52%——年报中数字/表格/拉丁字符 token 密度更高，实测约 1.33 字符/token。粗估口径仅用于闸门预检，记账一律以供应商 usage 为准 |
| 预算位置 | M0 上限 ¥20：累计实花 ≈ ¥0.699（embedding 0.6983 + 冒烟 0.000024） |

## 五、复现命令序列

```powershell
# 前提：corpus/raw 按 manifest 就位（_download_log.json 为采集底账）
uv run python -m scripts.corpus.parse_pdfs --raw corpus/raw --manifest corpus/manifest.json --out corpus/derived/parsed
uv run python -m scripts.corpus.chunk --parsed corpus/derived/parsed --manifest corpus/manifest.json --out corpus/derived/chunks.jsonl
# （embedding 一经取得即冻结：重建时直接复用 embeddings.jsonl，不重算）
uv run python -m scripts.corpus.finalize --derived corpus/derived --manifest corpus/manifest.json
uv run python -m scripts.corpus.load_pg --derived corpus/derived
```

封版纪律（08 六.5）：manifest.json / chunks.jsonl / embeddings.jsonl 三文件已进哈希，
自此任何改动 = 新版本重打哈希；一切评测数字只出自本 corpus_hash。
