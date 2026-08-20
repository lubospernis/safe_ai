from unittest.mock import patch

import pytest

from send_newsletter import check_newsletter_config, parse_report

_HEAD = """
<html><body>
<p class="meta">Slovakia &middot; Euro Area &middot; Germany | Generated 21 Jul 2026</p>
<h1>SAFE Report — Wave 39</h1>
<div class="exec-summary" id="exec-summary">
  <ul>
    <li><a href="#sec1"><strong>Confidence pulling back:</strong> a net 6.75% expected an increase</a></li>
  </ul>
</div>
"""

_REGULAR_SECTION = """
<section id="sec1">
  <h3>Confidence pulling back</h3>
  <p class="section-subtitle">Business Situation Indicators (Q2)</p>
  <ul><li>bullet</li></ul>
</section>
"""

_TAIL = "</body></html>"


def test_parse_report_extracts_regular_section_finding():
    html = _HEAD + _REGULAR_SECTION + _TAIL
    data = parse_report(html)
    assert data["findings"] == [
        {"finding": "Confidence pulling back", "subtitle": "Business Situation Indicators (Q2)"}
    ]


# ── check_newsletter_config (2026-07-21) ─────────────────────────────────────
# Exists to catch the exact two failures a real send hit today (malformed
# SUPABASE_URL -> PGRST125; missing allowed_emails.lang -> 42703) before a
# real send does, since get_subscribers() had never been exercised in
# production until then.

def test_check_newsletter_config_exits_zero_when_supabase_reachable(tmp_path):
    fake_report = tmp_path / "report_latest.html"
    fake_report.write_text(_HEAD + _REGULAR_SECTION + _TAIL, encoding="utf-8")
    with patch("send_newsletter.get_subscribers", return_value=[{"email": "a@b.com", "lang": "sk"}]), \
         patch("send_newsletter.REPORT_HTML", fake_report):
        with pytest.raises(SystemExit) as exc:
            check_newsletter_config()
    assert exc.value.code == 0


def test_check_newsletter_config_exits_nonzero_when_supabase_fails(tmp_path):
    with patch("send_newsletter.get_subscribers", side_effect=RuntimeError("PGRST125")), \
         patch("send_newsletter.REPORT_HTML", tmp_path / "missing.html"):
        with pytest.raises(SystemExit) as exc:
            check_newsletter_config()
    assert exc.value.code == 1


def test_check_newsletter_config_does_not_require_gmail_env(tmp_path, monkeypatch):
    """Deliberately independent of GMAIL_ADDRESS/GMAIL_16CHAR — the point is to
    validate the Supabase/report layer, which sending doesn't need at all."""
    monkeypatch.delenv("GMAIL_ADDRESS", raising=False)
    monkeypatch.delenv("GMAIL_16CHAR", raising=False)
    with patch("send_newsletter.get_subscribers", return_value=[]), \
         patch("send_newsletter.REPORT_HTML", tmp_path / "missing.html"):
        with pytest.raises(SystemExit) as exc:
            check_newsletter_config()
    assert exc.value.code == 0


def test_check_newsletter_config_ok_when_no_report_yet(tmp_path):
    """No report file yet (e.g. run before the first ever report) is a WARN,
    not a FAIL — only a genuine Supabase problem should fail the check."""
    with patch("send_newsletter.get_subscribers", return_value=[]), \
         patch("send_newsletter.REPORT_HTML", tmp_path / "missing.html"):
        with pytest.raises(SystemExit) as exc:
            check_newsletter_config()
    assert exc.value.code == 0
