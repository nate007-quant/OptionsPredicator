from __future__ import annotations

from pathlib import Path

from options_ai.utils_web.tail import tail_lines, tail_jsonl


def test_tail_lines_small(tmp_path: Path):
    p = tmp_path / "x.log"
    p.write_text("\n".join([f"line{i}" for i in range(1, 51)]) + "\n", encoding="utf-8")

    out = tail_lines(p, limit=10)
    assert out[0].strip() == "line41"
    assert out[-1].strip() == "line50"


def test_tail_jsonl_parsing(tmp_path: Path):
    p = tmp_path / "j.log"
    p.write_text('{"a":1}\nnotjson\n{"b":2}\n', encoding="utf-8")

    out = tail_jsonl(p, limit=10)
    assert out[0]["parsed"] == {"a": 1}
    assert out[1]["parsed"] is None
    assert out[2]["parsed"] == {"b": 2}
