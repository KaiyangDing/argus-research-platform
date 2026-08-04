"""批量 embedding（M0.9，[AI写]）：断点续跑 + 费用上限，超限即停（05 §11.3）。

CLI：uv run python -m scripts.corpus.embed --chunks corpus/derived/chunks.jsonl
     --out corpus/derived/embeddings.jsonl --max-yuan 15
前置：ARGUS_ENV=demo、ARGUS_LLM_MODE=live（dev 强制 replay，02 §6.3）。
产出行 = {chunk_id, vector}；向量原样保存 API 返回值——embedding 一经取得即冻结，
评测期与重建均不重算（ADR-008 §2）。
"""

import argparse
import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path

from argus.core.config import ArgusSettings
from argus.core.llm import BudgetExceededError, BudgetGate, CallContext, LLMClient


async def run(chunks_path: Path, out_path: Path, max_yuan: Decimal) -> int:
    settings = ArgusSettings()
    if settings.llm_mode != "live":
        print("embedding 跑批需 ARGUS_ENV=demo、ARGUS_LLM_MODE=live（07 §8）")
        return 2

    rows = [
        json.loads(line)
        for line in chunks_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    done: set[str] = set()
    if out_path.exists():
        for line in out_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                done.add(str(json.loads(line)["chunk_id"]))
    todo = [r for r in rows if r["chunk_id"] not in done]
    print(f"总块数 {len(rows)}，已完成 {len(done)}，待跑 {len(todo)}")

    gate = BudgetGate("m0-embedding", max_yuan)
    client = LLMClient(settings, gates=[gate])
    ctx = CallContext(scenario_id="corpus-build", node_id="corpus")
    step = settings.embed_batch_size
    written = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with out_path.open("a", encoding="utf-8", newline="\n") as f:
            for i in range(0, len(todo), step):
                batch = todo[i : i + step]
                vectors = await client.embed([str(r["text"]) for r in batch], ctx)
                for row, vec in zip(batch, vectors, strict=True):
                    f.write(json.dumps({"chunk_id": row["chunk_id"], "vector": vec}) + "\n")
                f.flush()  # 每批落盘：中断重跑不重复花钱（断点续跑按 chunk_id）
                written += len(batch)
                if written % 200 < step:
                    print(f"  进度 {len(done) + written}/{len(rows)}，累计花费 ¥{gate.spent_yuan}")
    except BudgetExceededError as exc:
        print(f"费用上限触发，中止（已写 {written} 块可续跑）：{exc}")
        return 2
    finally:
        await client.aclose()
    print(f"完成：总 {len(rows)} / 本次新增 {written} / 累计花费 ¥{gate.spent_yuan}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="chunks.jsonl → embeddings.jsonl（断点续跑）")
    parser.add_argument("--chunks", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--max-yuan", default="15")
    args = parser.parse_args(argv)
    return asyncio.run(run(args.chunks, args.out, Decimal(args.max_yuan)))


if __name__ == "__main__":
    sys.exit(main())
