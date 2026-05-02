from __future__ import annotations

import re
from pathlib import Path


def test_no_trading_write_operations():
    root = Path(__file__).parents[1] / "src" / "polymarket_insight"
    forbidden = [
        "create" + "_and" + "_post" + "_order",
        "post" + "_order",
        "cancel" + "_order",
        "PRIVATE" + "_KEY",
    ]
    text = "\n".join(path.read_text(encoding="utf-8") for path in root.rglob("*.py"))

    for token in forbidden:
        assert token not in text
    assert re.search(r"(?<!as)sign\(", text) is None
