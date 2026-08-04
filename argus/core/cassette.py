"""cassette 录制回放（M0.7）：三模式地基。

匹配键（全项目冻结，速查表 H.4）：(scenario_id, node_id, attempt, call_index)——
scenario_id 是带子文件坐标（目录名），其余三元组是带内条目坐标（节点局部序）。
缺带 = CassetteMissingError（绝不静默转真实调用）；digest 不符 = CassetteMismatchError
（禁止"旧带子配新词"，07 §8 版本绑定）。
"""

import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel

from argus.core.llm import BudgetExceededError
from argus.core.types import ArgusError

_SECRET_PATTERN = re.compile(r"sk-[A-Za-z0-9\-_]{8,}")
_SECRET_KEYS = {"authorization", "api_key", "organization"}
_MASK = "«scrubbed»"


class CassetteMissingError(ArgusError):
    """缺带：需要一次录制会话（≤¥5），等用户同意——绝不静默转真实调用。"""


class CassetteMismatchError(ArgusError):
    """key 或 request_digest 对不上：prompt/参数变更需重录（07 §8 版本绑定）。"""


@dataclass(frozen=True, slots=True, order=True)
class CassetteKey:
    node_id: str
    attempt: int
    call_index: int  # 同 (scenario_id, node_id, attempt) 内从 0 递增的局部序


class CassetteHeader(BaseModel):
    scenario_id: str
    corpus_hash: str | None  # M0 冒烟时语料未封版，允许 None【实现细则】
    models: list[str]
    recorded_at: str  # ISO 时间，仅元数据，不参与匹配
    spent_yuan: str


class CassetteEntry(BaseModel):
    key: CassetteKey
    endpoint: Literal["chat", "embeddings"]
    request_digest: str
    response_text: str | None = None
    response_vectors: list[list[float]] | None = None
    model: str
    request_id: str | None = None
    usage: dict[str, int]  # input / output / cached
    cost_yuan: str


class Cassette(BaseModel):
    header: CassetteHeader
    entries: list[CassetteEntry]


def scrub(obj: Any) -> Any:
    """录制清洗（07 §8）：落盘前强制过一遍——清洗不过就不落盘。"""
    if isinstance(obj, dict):
        return {k: _MASK if k.lower() in _SECRET_KEYS else scrub(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [scrub(v) for v in obj]
    if isinstance(obj, str):
        return _SECRET_PATTERN.sub(_MASK, obj)
    return obj


class CassetteStore:
    def __init__(self, root: Path = Path("tests/cassettes")) -> None:
        self._root = root

    def _path(self, scenario_id: str) -> Path:
        return self._root / scenario_id / "cassette.json"

    def load(self, scenario_id: str) -> Cassette:
        path = self._path(scenario_id)
        if not path.exists():
            raise CassetteMissingError(
                f"缺 cassette：{path}——需要一次录制会话（≤¥5），等用户同意后录制"
            )
        return Cassette.model_validate_json(path.read_text(encoding="utf-8"))

    def save(self, cassette: Cassette) -> None:
        path = self._path(cassette.header.scenario_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        payload = json.dumps(scrub(cassette.model_dump(mode="json")), ensure_ascii=False, indent=2)
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)  # 原子替换：写一半的带子不会被读到


class CassettePlayer:
    def __init__(self, cassette: Cassette) -> None:
        self._entries = {e.key: e for e in cassette.entries}

    def replay(self, key: CassetteKey, request_digest: str) -> CassetteEntry:
        entry = self._entries.get(key)
        if entry is None:
            raise CassetteMissingError(f"带内无此调用坐标：{key}（需重录或检查调用序）")
        if entry.request_digest != request_digest:
            raise CassetteMismatchError(
                f"request_digest 不符：{key}——prompt/参数已变更，按 07 §8 重录相关 cassette"
            )
        return entry


class CassetteRecorder:
    def __init__(
        self,
        store: CassetteStore,
        scenario_id: str,
        *,
        corpus_hash: str | None,
        session_cap_yuan: Decimal,
    ) -> None:
        self._store = store
        self._scenario_id = scenario_id
        self._corpus_hash = corpus_hash
        self._cap = session_cap_yuan
        self._spent = Decimal(0)
        self._entries: list[CassetteEntry] = []

    @property
    def has_entries(self) -> bool:
        return bool(self._entries)

    def record(
        self,
        key: CassetteKey,
        *,
        endpoint: Literal["chat", "embeddings"],
        request_digest: str,
        model: str,
        usage: dict[str, int],
        cost_yuan: Decimal,
        response_text: str | None = None,
        response_vectors: list[list[float]] | None = None,
        request_id: str | None = None,
    ) -> None:
        self._spent += cost_yuan  # 先入账（钱已花）再检查：超限中止会话（07 §8）
        if self._spent > self._cap:
            raise BudgetExceededError("record-session", self._cap, self._spent, Decimal(0))
        self._entries.append(
            CassetteEntry(
                key=key,
                endpoint=endpoint,
                request_digest=request_digest,
                response_text=response_text,
                response_vectors=response_vectors,
                model=model,
                request_id=request_id,
                usage=usage,
                cost_yuan=str(cost_yuan),
            )
        )

    def finalize(self, *, recorded_at: str | None = None) -> Cassette:
        cassette = Cassette(
            header=CassetteHeader(
                scenario_id=self._scenario_id,
                corpus_hash=self._corpus_hash,
                models=sorted({e.model for e in self._entries}),
                recorded_at=recorded_at or datetime.now(UTC).isoformat(),
                spent_yuan=str(self._spent),
            ),
            entries=list(self._entries),
        )
        self._store.save(cassette)
        return cassette
