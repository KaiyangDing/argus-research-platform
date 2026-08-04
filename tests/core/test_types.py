"""M0.4 公共类型测试（施工手册 M0.4 测试清单）。"""

from argus.core.types import ModelTier


def test_model_tier_values_frozen() -> None:
    # 速查表 D.1 #28：flash → plus → max，值冻结，禁止增删改
    assert [t.value for t in ModelTier] == ["flash", "plus", "max"]
