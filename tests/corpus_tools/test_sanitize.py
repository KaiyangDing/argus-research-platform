"""M0.8 脱敏测试（施工手册 M0.8 测试清单）。合成文本，不碰真实语料。"""

from scripts.corpus.sanitize import MASK, flag_address_lines, sanitize_text


def test_id_number_masked() -> None:
    clean, stats = sanitize_text("法定代表人身份证号：11010519491231002X，备案在册。")
    assert "11010519491231002X" not in clean
    assert MASK in clean
    assert stats.id_number == 1


def test_phone_masked() -> None:
    clean, stats = sanitize_text(
        "联系方式：13812345678，座机 0571-88123456，邮箱 ir@yunshan.example.com。"
    )
    assert "13812345678" not in clean
    assert "0571-88123456" not in clean
    assert "ir@yunshan.example.com" not in clean
    assert stats.mobile == 1
    assert stats.landline == 1
    assert stats.email == 1


def test_company_name_preserved_and_address_flagged() -> None:
    text = "杭州云杉电商有限公司为被告。\n住所地：杭州市西湖区某某路 1 号。\n经营范围：电子商务。"
    clean, stats = sanitize_text(text)
    assert "杭州云杉电商有限公司" in clean  # 法人名称保留
    assert stats.total() == 0
    flags = flag_address_lines(text)
    assert flags == ["住所地：杭州市西湖区某某路 1 号。"]
    assert "住所地：杭州市西湖区某某路 1 号。" in clean  # 只标记，不改文本
