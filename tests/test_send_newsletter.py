from send_newsletter import parse_report

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


def test_parse_report_excludes_adhoc_sub_sections_variant():
    """html_builder.py's sub_sections shape: <section id="adhoc_spotlight"> wrapping
    multiple <div class="ai-sub-section"> blocks, each with its own h3. Must not
    mislabel the first sub-section as a top-level finding, and must not drop the
    rest either — the whole adhoc block is simply excluded from this digest."""
    adhoc = """
    <section id="adhoc_spotlight" data-theme="AI adoption">
      <h2>Special Focus: AI adoption</h2>
      <div class="ai-sub-section">
        <h3>Current AI use</h3>
        <p class="section-subtitle">QA1/QA4</p>
        <ul><li>bullet A</li></ul>
      </div>
      <div class="ai-sub-section">
        <h3>Drivers and barriers</h3>
        <p class="section-subtitle">QA2/QA3</p>
        <ul><li>bullet B</li></ul>
      </div>
    </section>
    """
    html = _HEAD + _REGULAR_SECTION + adhoc + _TAIL
    data = parse_report(html)
    assert data["findings"] == [
        {"finding": "Confidence pulling back", "subtitle": "Business Situation Indicators (Q2)"}
    ]


def test_parse_report_excludes_adhoc_per_question_variant():
    """html_builder.py's per-question shape: <div id="adhoc_spotlight"> wrapping one
    real <section id="qid"> per adhoc question — each of those WOULD otherwise match
    `section[id]` and get pulled in as if it were a regular section."""
    adhoc = """
    <div id="adhoc_spotlight" data-theme="AI adoption">
      <h2>Special Focus: AI adoption</h2>
      <p class="section-subtitle">Overview</p>
      <section id="qa1" class="adhoc-question-section">
        <h3>Firms report growing AI use</h3>
        <p class="section-subtitle">QA1 — Has your firm used AI tools?</p>
        <ul><li>bullet</li></ul>
      </section>
    </div>
    """
    html = _HEAD + _REGULAR_SECTION + adhoc + _TAIL
    data = parse_report(html)
    assert data["findings"] == [
        {"finding": "Confidence pulling back", "subtitle": "Business Situation Indicators (Q2)"}
    ]


def test_parse_report_excludes_adhoc_flat_variant():
    """html_builder.py's flat fallback shape: <div id="adhoc_spotlight"> with a plain
    h3/subtitle and no nested <section> at all — never matched `section[id]` to begin
    with, so this is a no-op regression guard confirming that stays true."""
    adhoc = """
    <div id="adhoc_spotlight" data-theme="AI adoption">
      <h2>Special Focus: AI adoption</h2>
      <h3>Firms report growing AI use</h3>
      <p class="section-subtitle">AI adoption module</p>
      <ul><li>bullet</li></ul>
    </div>
    """
    html = _HEAD + _REGULAR_SECTION + adhoc + _TAIL
    data = parse_report(html)
    assert data["findings"] == [
        {"finding": "Confidence pulling back", "subtitle": "Business Situation Indicators (Q2)"}
    ]


def test_parse_report_exec_bullets_unaffected_by_adhoc_exclusion():
    adhoc = """
    <section id="adhoc_spotlight" data-theme="AI adoption">
      <h2>Special Focus: AI adoption</h2>
      <div class="ai-sub-section"><h3>X</h3><p class="section-subtitle">Y</p></div>
    </section>
    """
    html = _HEAD + _REGULAR_SECTION + adhoc + _TAIL
    data = parse_report(html)
    assert len(data["exec_bullets"]) == 1
    assert "Confidence pulling back" in data["exec_bullets"][0]
