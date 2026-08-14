import re
import uuid
from enum import StrEnum
from typing import Final, NewType

from pydantic import BaseModel, ConfigDict


class ArgusError(Exception):
    """全项目异常基类：argus 包内自定义异常一律继承它。"""


TaskId = NewType("TaskId", uuid.UUID)
NodeId = NewType("NodeId", uuid.UUID)

# 契约 id：形状 name@version（03 §1.1 边契约；本地只校形状，注册表校验 M3 接入）
ContractId = NewType("ContractId", str)
CONTRACT_ID_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z][a-z0-9_]*@[1-9][0-9]*$")


def parse_contract_id(raw: str) -> tuple[str, int]:
    """拆 ContractId 为 (name, version)；形状非法抛 ValueError。"""
    if CONTRACT_ID_RE.match(raw) is None:
        raise ValueError(f"illegal contract id: {raw!r}")
    name, _, version = raw.partition("@")
    return name, int(version)


class RoleName(StrEnum):  # 速查表 D.1 #5，五值逐字（M1.1 由 Literal 升 StrEnum，M0 零引用）
    PLANNER = "planner"
    RESEARCHER = "researcher"
    ANALYST = "analyst"
    VERIFIER = "verifier"
    SYNTHESIZER = "synthesizer"


class ArtifactKind(StrEnum):  # 速查表 D.1 #6，六值逐字
    PLAN_SNAPSHOT = "plan_snapshot"
    RESEARCH_NOTE = "research_note"
    ANALYSIS_TABLE = "analysis_table"
    VERIFICATION_VERDICT = "verification_verdict"
    REPORT_DRAFT = "report_draft"
    REPORT_FINAL = "report_final"


class Granularity(StrEnum):  # 速查表 D.1 #7，三值逐字：工件读侧粒度
    HEADLINE = "headline"
    DIGEST = "digest"
    FULL = "full"


class ModelTier(StrEnum):  # 速查表 D.1 #28；放 types 避免 config↔llm 循环依赖
    FLASH = "flash"
    PLUS = "plus"
    MAX = "max"


class ArtifactRef(BaseModel):
    """工件引用（03 §8.2 字段逐字）；下沉 core 供 engine/bus 平级引用（M1 册冲突 C-3 口径）。"""

    model_config = ConfigDict(frozen=True, extra="forbid")

    artifact_id: uuid.UUID
    granularity: Granularity
