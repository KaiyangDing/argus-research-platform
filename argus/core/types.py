import uuid
from enum import StrEnum
from typing import Literal, NewType


class ArgusError(Exception):
    """全项目异常基类：argus 包内自定义异常一律继承它。"""


TaskId = NewType("TaskId", uuid.UUID)
NodeId = NewType("NodeId", uuid.UUID)
RoleName = Literal["planner", "researcher", "analyst", "verifier", "synthesizer"]  # 速查表 D.1 #5


class ModelTier(StrEnum):  # 速查表 D.1 #28；放 types 避免 config↔llm 循环依赖
    FLASH = "flash"
    PLUS = "plus"
    MAX = "max"
