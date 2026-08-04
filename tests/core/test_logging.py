"""M0.4 结构化日志测试（施工手册 M0.4 测试清单）。"""

import json

import pytest
from pydantic import SecretStr

from argus.core.logging import configure_logging, get_logger


def test_json_output_with_bound_fields(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    get_logger("test").bind(task_id="t1", node_id="n1").info("x")
    line = capsys.readouterr().out.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["task_id"] == "t1"
    assert payload["node_id"] == "n1"
    assert payload["event"] == "x"


def test_secretstr_masked_in_log(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging()
    get_logger("test").info("llm_call", api_key=SecretStr("sk-abc"))
    out = capsys.readouterr().out
    assert "sk-abc" not in out
    assert "llm_call" in out
