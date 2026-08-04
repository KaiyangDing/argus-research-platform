"""脱敏函数库（M0.8，[AI写]）：正则清洗 + 住址行标记（07 §10.4 口径）。

只清洗自然人隐私痕迹（身份证/手机/固话/邮箱）；法人名称保留——研究对象就是公司；
住址行只标记进人工抽查清单、不自动改（住址正则误伤率高，人工抽查是 07 §10.4 的本意）；
自然人姓名化名在演示层处理，语料层不改原文。
"""

import re
from dataclasses import dataclass

MASK = "［已脱敏］"

_ID_18 = re.compile(r"(?<!\d)\d{17}[\dXx](?![\dXx])")
_ID_15 = re.compile(r"(?<!\d)\d{15}(?!\d)")
_MOBILE = re.compile(r"(?<!\d)1[3-9]\d{9}(?!\d)")
_LANDLINE = re.compile(r"(?<!\d)0\d{2,3}-\d{7,8}(?!\d)")
_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_ADDRESS_HINTS = ("住所地", "家庭住址", "住址")


@dataclass
class SanitizeStats:
    id_number: int = 0
    mobile: int = 0
    landline: int = 0
    email: int = 0

    def total(self) -> int:
        return self.id_number + self.mobile + self.landline + self.email

    def add(self, other: SanitizeStats) -> None:
        self.id_number += other.id_number
        self.mobile += other.mobile
        self.landline += other.landline
        self.email += other.email


def sanitize_text(text: str) -> tuple[str, SanitizeStats]:
    """身份证（18/15 位）→ 手机 → 固话 → 邮箱，顺序固定（先长后短防子串误配）。"""
    stats = SanitizeStats()
    text, n = _ID_18.subn(MASK, text)
    stats.id_number += n
    text, n = _ID_15.subn(MASK, text)
    stats.id_number += n
    text, n = _MOBILE.subn(MASK, text)
    stats.mobile += n
    text, n = _LANDLINE.subn(MASK, text)
    stats.landline += n
    text, n = _EMAIL.subn(MASK, text)
    stats.email += n
    return text, stats


def flag_address_lines(text: str) -> list[str]:
    """住址启发式：只返回可疑行供人工抽查，不修改文本。"""
    return [line for line in text.splitlines() if any(h in line for h in _ADDRESS_HINTS)]
