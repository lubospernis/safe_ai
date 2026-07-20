import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import quality_check

_LONG_BULLET = (
    "Net balance worsened from 5.0pp in wave 37 to 7.0pp in wave 38 for Slovak firms, "
    "while the EA figure moved from 3.0pp to 5.0pp over the same period, and German firms "
    "similarly saw their net balance rise from 1.5pp to 3.5pp, with all three economies "
    "showing a consistent upward trend across the two waves of collected survey data."
)
_SHORT_BULLET = "Net balance worsened to 7.0pp this wave for Slovak firms, a modest tightening."


def _html(bullets: list[str]) -> str:
    items = "".join(f"<li>{b}</li>" for b in bullets)
    return (
        f"<html><body>"
        f"<section id='exec-summary'><h2>Executive Summary</h2><ul>{items}</ul></section>"
        f"<section id='sec1'><h3>Section One</h3><ul>{items}</ul></section>"
        f"</body></html>"
    )


def _supervisor_response(readability=9, substance=9, coherence=9, sign_convention=9, verdict="pass"):
    resp = MagicMock()
    resp.choices[0].message.content = json.dumps({
        "readability": readability, "substance": substance, "coherence": coherence,
        "sign_convention": sign_convention, "verdict": verdict, "reason": "test",
    })
    return resp


def _run_main(tmp_path, monkeypatch, html: str, supervisor_resp):
    html_path = tmp_path / "report_test.html"
    html_path.write_text(html, encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["quality_check.py", "--html", str(html_path)])
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key")

    client = MagicMock()
    client.chat.complete.return_value = supervisor_resp
    with patch("quality_check.Mistral", return_value=client):
        with pytest.raises(SystemExit) as exc_info:
            quality_check.main()

    scores_path = tmp_path / "quality_scores_test.json"
    scores = json.loads(scores_path.read_text())
    return exc_info.value.code, scores


def test_tier1_only_does_not_block_publish(tmp_path, monkeypatch):
    """A style-only violation (bullet too long) with a clean supervisor score
    must exit 3 (non-blocking), not 1 — this is the exact incident this change
    fixes: 22 length violations should never have killed the whole run."""
    html = _html([_SHORT_BULLET, _LONG_BULLET])
    code, scores = _run_main(tmp_path, monkeypatch, html, _supervisor_response())

    assert code == 3
    assert scores["tier1_issue_count"] > 0
    assert scores["tier2_fail"] is False


def test_tier2_failure_blocks_publish(tmp_path, monkeypatch):
    """A genuinely bad supervisor score is a real content problem — must
    exit 1 and set tier2_fail True, regardless of style cleanliness."""
    html = _html([_SHORT_BULLET])
    code, scores = _run_main(
        tmp_path, monkeypatch, html,
        _supervisor_response(readability=2, verdict="fail"),
    )

    assert code == 1
    assert scores["tier2_fail"] is True


def test_clean_report_passes(tmp_path, monkeypatch):
    html = _html([_SHORT_BULLET])
    code, scores = _run_main(tmp_path, monkeypatch, html, _supervisor_response())

    assert code == 0
    assert scores["tier1_issue_count"] == 0
    assert scores["tier2_fail"] is False


def test_supervisor_outage_preserves_tier1_signal(tmp_path, monkeypatch):
    """If the Mistral supervisor call itself fails (API outage), that must not
    be indistinguishable from a real failure — but it also must not silently
    swallow a tier-1 finding that was already computed locally before the
    call was attempted."""
    html = _html([_SHORT_BULLET, _LONG_BULLET])
    html_path = tmp_path / "report_test.html"
    html_path.write_text(html, encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["quality_check.py", "--html", str(html_path)])
    monkeypatch.setenv("MISTRAL_API_KEY", "test-key")

    client = MagicMock()
    client.chat.complete.side_effect = RuntimeError("connection reset")
    with patch("quality_check.Mistral", return_value=client):
        with pytest.raises(SystemExit) as exc_info:
            quality_check.main()

    scores = json.loads((tmp_path / "quality_scores_test.json").read_text())
    assert exc_info.value.code == 3
    assert scores["tier1_issue_count"] > 0
    assert scores["tier2_fail"] is False
    assert scores["verdict"] == "pass"
