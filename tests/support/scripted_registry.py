"""worker --registry 注入用工厂（M1 册 M1.10；冻结区 2.5 #18：仅测试/演练用途）。

生产注册表 M3 由 roster 提供。chaos 演练（M1.14）经 ARGUS_SCRIPT_TABLE 指定剧本表
JSON 文件：{"<node-uuid>": {"<attempt>": {ScriptEntry 字段}}}；缺省一律 success。
"""

import json
import os
import uuid
from decimal import Decimal
from pathlib import Path

from argus.core.types import NodeId, RoleName
from argus.engine.ports import BudgetHint, ContractPair, ExecutorRegistry, NodeExecutor
from tests.support.scripted_executor import ScriptedExecutor, ScriptEntry


class ScriptedRegistry:
    """所有 role 解析到同一 ScriptedExecutor；BudgetHint 固定小值（M1 无预算消费方）。"""

    def __init__(self, executor: ScriptedExecutor) -> None:
        self._executor = executor

    def resolve(self, role: RoleName) -> tuple[NodeExecutor, ContractPair, BudgetHint]:
        return self._executor, ContractPair(), BudgetHint(est_tokens=100, est_yuan=Decimal("0.01"))


def build_from_env() -> ExecutorRegistry:
    """CLI 注入入口：python -m argus.engine.worker --registry tests.support.scripted_registry:build_from_env"""
    script: dict[tuple[NodeId, int], ScriptEntry] = {}
    path = os.environ.get("ARGUS_SCRIPT_TABLE")
    if path:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        for node_str, by_attempt in raw.items():
            for attempt_str, entry in by_attempt.items():
                script[(NodeId(uuid.UUID(node_str)), int(attempt_str))] = (
                    ScriptEntry.model_validate(entry)
                )
    return ScriptedRegistry(ScriptedExecutor(script, seed=42))
