"""HTML assembly: page template, section rendering, TOC, annex, painting."""

import base64
import re
import textwrap
from datetime import date

ARTWORK = {
    "page_url": "https://www.webumenia.sk/dielo/SVK:SNG.IM_127",
    "img_url":  "https://www.webumenia.sk/dielo/nahlad/SVK:SNG.IM_127/600",
    "title":    "Július Koller — Pre každú príležitosť... osviežujúci národný podnik. (UFO) (1978)",
}

ROUTED_FOOTNOTE = (
    "<p class=\"footnote\">* Only firms that have used or applied for this type of financing "
    "in the past are asked this question. A lower n relative to the total sample is by design "
    "and does not indicate a data quality issue — see ECB SAFE methodology for details.</p>"
)

MISSINGNESS_FOOTNOTE = (
    "<p class=\"footnote\">† Observations with fewer than 10 valid responses in a given "
    "wave × country × sub-item cell are excluded from the chart; gaps in series indicate "
    "insufficient data for that period.</p>"
)

_AGENTIC_FOOTNOTE = (
    '<p class="footnote">🤖 This section includes data retrieved by an AI agent '
    'querying the SAFE database directly during report generation.</p>\n'
)

_SK_UI = {
    "lang":            "sk",
    "title":           "ECB SAFE Survey — Vlna {wave} · Slovensko",
    "h1":              "ECB SAFE Survey — Vlna {wave}",
    "meta":            "Slovensko · Eurozóna · Nemecko &nbsp;|&nbsp; Vygenerované {date}",
    "exec_h2":         "Zhrnutie",
    "toc_title":       "Obsah",
    "group_financing": "Podmienky financovania",
    "group_economic":  "Ekonomická situácia firiem",
    "footer": (
        "Zdroj: ECB SAFE mikrodáta. Čistá bilancia = % podnikov hlásiacich nárast "
        "mínus % podnikov hlásiacich pokles. Kladná hodnota = sprísnenie / rast "
        "(nepriaznivé pre firmy, ak nie je uvedené inak). Záporná hodnota = uvoľnenie / pokles."
    ),
    "footnote_routed": (
        "<p class=\"footnote\">* Túto otázku dostávajú iba firmy, ktoré v minulosti využili "
        "alebo žiadali o daný typ financovania. Nižší počet respondentov oproti celkovej vzorke "
        "je zámerný a neindikuje problém s kvalitou dát — pozri metodológiu ECB SAFE.</p>"
    ),
    "footnote_missing": (
        "<p class=\"footnote\">† Bunky s menej ako 10 platnými odpoveďami v danej kombinácii "
        "vlny × krajiny × položky sú vynechané z grafu; medzery v sérii indikujú nedostatok dát "
        "pre dané obdobie.</p>"
    ),
    "footnote_agentic": (
        "<p class=\"footnote\">🤖 Táto sekcia obsahuje dáta získané AI agentom priamym "
        "dopytovaním databázy SAFE počas generovania správy.</p>\n"
    ),
    "adhoc_special_focus":   "Špeciálna téma",
    "adhoc_read_more":       "Čítaj viac:",
    "adhoc_ecb_article":     "Článok ECB Economic Bulletin",
    "adhoc_all_questions":   "Všetky adhoc otázky",
    "annex_summary":       "Otázky zbierané na 3-mesačnej báze",
    "annex_col_topic":     "Téma",
    "annex_col_id":        "ID",
    "annex_col_question":  "Otázka",
    "annex_col_module":    "Modul",
    "key_finding_label":   "Kľúčové zistenie:",
    "interest_label":      " (záujem: {score}/5)",
    "chart_alt_suffix":    "graf",
    "annex_groups": {
        "Business situation":                 "Obchodná situácia",
        "Financing needs &amp; availability": "Potreba a dostupnosť financovania",
        "Credit supply factors":              "Faktory ponuky úveru",
        "Financing conditions &amp; terms":   "Podmienky financovania",
        "Financing applications":             "Žiadosti o financovanie",
        "Outlook &amp; expectations":         "Výhľad a očakávania",
    },
}

ANNEX_GROUPS = [
    ("Business situation", ["Q0b", "Q2"]),
    ("Financing needs &amp; availability", ["Q4", "Q5", "Q9"]),
    ("Credit supply factors", ["Q11"]),
    ("Financing conditions &amp; terms", ["Q10", "Q23"]),
    ("Financing applications", ["Q7A", "Q7B", "Q6A"]),
    ("Outlook &amp; expectations", ["Q31", "Q33", "Q34"]),
]
ANNEX_Q_IDS = {q for _, qs in ANNEX_GROUPS for q in qs}

GROUP_ORDER = ["Financing Conditions", "Economic Situation of Firms"]

HTML_PAGE = textwrap.dedent("""
<!DOCTYPE html>
<html lang="{lang}">
<head>
<meta charset="UTF-8">
<title>{title_str}</title>
<style>
  /* NBS brand: Sitka Banner for headings, Arial for body */
  body        {{ font-family: Arial, sans-serif; background: #f4f4f4; color: #231f20;
                 max-width: 1200px; margin: 40px auto; padding: 0 24px; }}
  h1          {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-size: 26px; font-weight: bold; margin-bottom: 4px; color: #2B5291; }}
  .meta       {{ color: #6a6a6a; font-size: 13px; margin-bottom: 20px; }}
  section     {{ background: #fff; border: 1px solid #D2DBE0; border-radius: 6px;
                 padding: 24px 28px; margin-bottom: 20px; }}
  h2          {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-size: 18px; font-weight: bold; margin: 36px 0 12px 0; color: #2B5291;
                 border-bottom: 2px solid #2B5291; padding-bottom: 6px; }}
  h3          {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-size: 15px; font-weight: bold; margin: 0 0 4px 0; color: #231f20; }}
  .section-subtitle {{ font-size: 11px; color: #888; margin: 0 0 12px 0; }}
  ul          {{ padding-left: 20px; margin: 0 0 16px 0; }}
  li          {{ margin-bottom: 6px; font-size: 13.5px; line-height: 1.5; }}
  .chart-img  {{ display: block; max-width: 560px; width: 100%; margin-top: 8px; }}
  .chart-img.chart-img--adhoc {{ margin: 8px 0 10px; }}
  .chart-img.chart-img--flex-third {{ max-width: calc(34% - 0.5rem); min-width: 220px; flex: 1 1 220px; }}
  .footnote   {{ font-size: 11px; color: #888; margin-top: 10px; line-height: 1.4; }}
  .footer     {{ color: #adadad; font-size: 11px; margin-top: 32px; text-align: center; }}
  .lang-switch {{ float: right; font-size: 12px; color: #2B5291; text-decoration: none;
                  border: 1px solid #2B5291; border-radius: 4px; padding: 2px 8px;
                  margin-top: 4px; }}
  .lang-switch:hover {{ background: #eef2f9; }}

  /* Exec summary + painting flexbox */
  .exec-flex     {{ display: flex; gap: 24px; align-items: flex-start; margin-bottom: 20px; }}
  .exec-painting {{ flex: 1; min-width: 0; }}
  .exec-summary  {{ flex: 3; min-width: 0; background: #eef2f9;
                    border-left: 4px solid #2B5291; padding: 20px 24px; border-radius: 6px; }}
  .exec-summary h2 {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                      font-size: 16px; color: #2B5291; border-bottom: none;
                      margin: 0 0 10px 0; padding-bottom: 0; }}
  .exec-summary li {{ font-size: 14px; line-height: 1.7; }}
  .exec-summary li a {{ color: inherit; text-decoration: underline dotted #7a9dc4; }}
  .exec-summary li a:hover {{ text-decoration: underline; color: #2B5291; }}

  /* TOC */
  #toc        {{ background: #fff; border: 1px solid #D2DBE0; border-radius: 6px;
                 padding: 16px 24px; margin-bottom: 20px; font-size: 13px; }}
  .toc-title  {{ font-family: "Sitka Banner", "Sitka Text", Georgia, serif;
                 font-weight: bold; margin: 0 0 8px 0; color: #2B5291; font-size: 13px; }}
  #toc ul     {{ margin: 4px 0; padding-left: 18px; }}
  #toc li     {{ margin-bottom: 3px; }}
  #toc a      {{ color: #0086DE; text-decoration: none; }}
  #toc a:hover {{ text-decoration: underline; }}

  /* Collapsible annex */
  details     {{ background: #fff; border: 1px solid #D2DBE0; border-radius: 6px;
                 padding: 14px 22px; margin-bottom: 20px; }}
  summary     {{ font-weight: bold; font-size: 13px; cursor: pointer; color: #555;
                 user-select: none; }}
  summary:hover {{ color: #231f20; }}
  details table           {{ width: 100%; border-collapse: collapse; margin-top: 12px;
                             font-size: 12px; }}
  details td, details th  {{ padding: 5px 8px; border-bottom: 1px solid #f0f0f0;
                             vertical-align: top; }}
  details th              {{ font-weight: bold; background: #f8f8f8; text-align: left; }}
  .group-cell             {{ color: #888; font-style: italic; white-space: nowrap; }}
  .badge-common           {{ background: #e8f4e8; color: #2d7a00; padding: 1px 6px;
                             border-radius: 3px; font-size: 11px; }}
  .badge-ecb              {{ background: #eef2f9; color: #2B5291; padding: 1px 6px;
                             border-radius: 3px; font-size: 11px; }}
</style>
</head>
<body>
{lang_switch}<h1>{h1_str}</h1>
<p class="meta">{meta_str}</p>
{annex}
{exec_flex}
{toc}
{sections}
<p class="footer">{footer_str}</p>
</body>
</html>
""").strip()

SECTION_TMPL = textwrap.dedent("""
<section id="{section_id}">
  <h3>{finding}</h3>
  <p class="section-subtitle">{title}</p>
  <ul>
{bullets}
  </ul>
{footnote}{agentic_footnote}  <img class="chart-img" src="data:image/png;base64,{chart_b64}" alt="{title} {chart_alt_suffix}">
</section>
""").strip()


def render_section(
    *,
    section_id: str,
    headline: str,
    subtitle: str,
    bullets: list[str],
    chart_html: str = "",
    footnote: str = "",
    section_class: str = "",
) -> str:
    """Canonical section shape used by every report — main and adhoc alike:

        <h3>headline</h3>          (the story — a full-sentence finding, never a raw label)
        <p class="section-subtitle">subtitle</p>   (topic/question this section is about)
        <ul>bullets</ul>
        chart(s) last

    New report types must render their sections through this function rather than
    hand-building section HTML, so the chart-after-bullets order and headline-as-story
    convention can't silently drift per report. `headline`/`subtitle` are inserted as-is
    (format/escape before calling); `bullets` are markdown-bold-converted automatically.
    """
    bullets_html = "\n".join(
        f"    <li>{_md_to_html(b.lstrip('• ').strip())}</li>"
        for b in bullets
    )
    bullets_block = f'  <ul>\n{bullets_html}\n  </ul>\n' if bullets_html else ""
    class_attr = f' class="{section_class}"' if section_class else ""
    return (
        f'<section id="{section_id}"{class_attr}>\n'
        f'  <h3>{headline}</h3>\n'
        f'  <p class="section-subtitle">{subtitle}</p>\n'
        f'{bullets_block}'
        f'{footnote}'
        f'{chart_html}'
        f'</section>'
    )


def _md_to_html(text: str) -> str:
    """Convert **bold** markdown to <strong> HTML tags."""
    return re.sub(r'\*\*(.+?)\*\*', lambda m: f'<strong>{m.group(1)}</strong>', text)


class _NoImagePlaceholder(Exception):
    """Raised when webumenia.sk serves its own 'no image available' placeholder with a
    200 status — a deterministic failure mode that retrying will not fix."""


def _fetch_painting_inner_html(max_attempts: int = 3, retry_delay: float = 2.0) -> str:
    """Fetch the quarterly artwork; return inner <img>+<span> HTML. Returns "" on failure.

    Retries on transient failures (network hiccups are the common case on CI
    runners) before giving up and gracefully omitting the whole block. Does NOT
    retry when webumenia.sk itself reports no image is available (see
    _NoImagePlaceholder) — that's a persistent server-side condition, not a
    transient one, so retrying would just waste time on a guaranteed repeat.
    """
    import time as _time

    import requests as _requests

    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = _requests.get(ARTWORK["img_url"], timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            # webumenia.sk returns HTTP 200 with its own "no image available" placeholder
            # when the real artwork can't be served — status-code checks alone miss this.
            content_disposition = resp.headers.get("Content-Disposition", "")
            if "no-image" in content_disposition.lower():
                raise _NoImagePlaceholder(
                    f"webumenia.sk served its no-image placeholder ({content_disposition!r})"
                )
            b64 = base64.b64encode(resp.content).decode()
            ct = resp.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
            title = ARTWORK["title"]
            page_url = ARTWORK["page_url"]
            return (
                f'<a href="{page_url}" target="_blank" title="{title}">'
                f'<img src="data:{ct};base64,{b64}" alt="{title}" '
                f'style="width:100%;border-radius:4px;border:1px solid #e0e0e0;display:block;">'
                f'</a>'
                f'<span style="font-size:9px;color:#aaa;display:block;margin-top:4px;'
                f'text-align:right;font-style:italic;line-height:1.3;">'
                f'<a href="{page_url}" target="_blank" style="color:#aaa;text-decoration:none;">'
                f'{title}</a></span>'
            )
        except _NoImagePlaceholder as e:
            print(f"  Warning: {e} — skipping thumbnail (not retrying, this is not transient)")
            return ""
        except Exception as e:
            last_err = e
            if attempt < max_attempts:
                print(f"  Warning: painting fetch attempt {attempt} failed ({e}) — retrying...")
                _time.sleep(retry_delay)
    print(f"  Warning: could not fetch painting after {max_attempts} attempts ({last_err}) — skipping thumbnail")
    return ""


def _clean_question_text(text: str) -> str:
    """Strip 'Qxx/Qxx_g1.' or 'Qxx.' prefix from question text."""
    text = re.sub(r'^[A-Za-z0-9]+(?:/[A-Za-z0-9_]+)*\.\s*', '', text)
    return text.strip()


def _load_annex_question_texts(con=None) -> dict[str, str]:
    """Return {q_id_lower: cleaned_question_text} from MotherDuck annex table."""
    if con is None:
        return {}
    try:
        cols_res = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main_safe' AND table_name = 'ref_safe__annex' "
            "ORDER BY ordinal_position"
        ).fetchall()
        if not cols_res:
            return {}
        all_cols = [r[0] for r in cols_res]
        try:
            notes_idx = all_cols.index("notes")
        except ValueError:
            notes_idx = 6
        wave_cols = all_cols[notes_idx + 1:]
        element_col = all_cols[1] if len(all_cols) > 1 else "element"
        q_item_col = all_cols[2] if len(all_cols) > 2 else "question_item"

        wave_sel = ", ".join(f'"{c}"' for c in wave_cols)
        rows_md = con.execute(
            f'SELECT "{q_item_col}", {wave_sel} '
            f'FROM main_safe.ref_safe__annex '
            f"WHERE \"{element_col}\" = 'question'"
        ).fetchall()

        texts: dict[str, str] = {}
        for row in rows_md:
            q_id = (row[0] or "").strip().lower()
            if not q_id:
                continue
            text = next((v for v in row[1:] if v and v.strip()), "")
            if text and q_id not in texts:
                texts[q_id] = _clean_question_text(text)

        print(f"  Loaded {len(texts)} question texts from MotherDuck annex table")
        return texts
    except Exception as exc:
        print(f"  Warning: MotherDuck annex table unavailable ({exc})")
        return {}


def build_annex_html(con=None, ui: dict | None = None, question_texts_override: dict | None = None) -> str:
    """question_texts_override: optional {q_id_lower: translated_text} — when a question's
    ID matches (case-insensitively), its text replaces the English text fetched from
    MotherDuck. Used to render a translated (e.g. Slovak) annex without a second live query."""
    _ui = ui or {}
    _override = question_texts_override or {}
    q_texts: dict[str, tuple[str, str]] = {}

    if con is None:
        return ""
    try:
        cols_res = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'main_safe' AND table_name = 'ref_safe__annex' "
            "ORDER BY ordinal_position"
        ).fetchall()
        if cols_res:
            all_cols = [r[0] for r in cols_res]
            try:
                notes_idx = all_cols.index("notes")
            except ValueError:
                notes_idx = 6
            wave_cols = all_cols[notes_idx + 1:]
            element_col = all_cols[1] if len(all_cols) > 1 else "element"
            q_item_col = all_cols[2] if len(all_cols) > 2 else "question_item"
            sample_col = all_cols[4] if len(all_cols) > 4 else "sample"
            wave_sel = ", ".join(f'"{c}"' for c in wave_cols)
            rows_md = con.execute(
                f'SELECT "{q_item_col}", "{sample_col}", {wave_sel} '
                f"FROM main_safe.ref_safe__annex "
                f"WHERE \"{element_col}\" = 'question'"
            ).fetchall()
            for row in rows_md:
                q_id_raw = (row[0] or "").strip()
                sample = (row[1] or "").strip()
                matched = next((k for k in ANNEX_Q_IDS if k.lower() == q_id_raw.lower()), None)
                if matched and matched not in q_texts:
                    text = next((v for v in row[2:] if v and v.strip()), "")
                    if text:
                        translated = _override.get(matched.lower())
                        final_text = translated if translated else _clean_question_text(text.strip())
                        q_texts[matched] = (sample, final_text)
    except Exception as exc:
        print(f"  Warning: MotherDuck annex table unavailable for HTML widget ({exc})")
        return ""

    annex_group_labels = _ui.get("annex_groups", {})
    group_rows = []
    for group_label, q_ids in ANNEX_GROUPS:
        display_label = annex_group_labels.get(group_label, group_label)
        first = True
        for q_id in q_ids:
            entry = q_texts.get(q_id)
            if not entry:
                continue
            sample, text = entry
            sample_badge = (
                '<span class="badge-ecb">ECB module</span>'
                if "ECB" in sample
                else '<span class="badge-common">Common</span>'
            )
            group_cell = f'<td class="group-cell">{display_label}</td>' if first else '<td class="group-cell"></td>'
            first = False
            group_rows.append(
                f"    <tr>{group_cell}"
                f'<td><strong>{q_id}</strong></td>'
                f"<td>{text[:200]}{'…' if len(text) > 200 else ''}</td>"
                f"<td>{sample_badge}</td></tr>"
            )

    if not group_rows:
        return ""

    rows_html = "\n".join(group_rows)
    annex_summary = _ui.get("annex_summary", "Survey questions collected on a 3-month basis")
    col_topic    = _ui.get("annex_col_topic",    "Topic")
    col_id       = _ui.get("annex_col_id",       "ID")
    col_question = _ui.get("annex_col_question", "Question")
    col_module   = _ui.get("annex_col_module",   "Module")
    return textwrap.dedent(f"""
<details>
  <summary>{annex_summary} ({len(q_texts)} questions)</summary>
  <table>
    <thead>
      <tr><th>{col_topic}</th><th>{col_id}</th><th>{col_question}</th><th>{col_module}</th></tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
  </table>
</details>
""").strip()


def build_toc(rendered_sections: list[dict], ui: dict | None = None) -> str:
    _ui = ui or {}
    group_labels = {
        "Financing Conditions":        _ui.get("group_financing", "Financing Conditions"),
        "Economic Situation of Firms": _ui.get("group_economic",  "Economic Situation of Firms"),
    }
    toc_title = _ui.get("toc_title", "Contents")

    by_group: dict[str, list[dict]] = {}
    for s in rendered_sections:
        g = s.get("group", "Other")
        by_group.setdefault(g, []).append(s)

    items = []
    for group in GROUP_ORDER:
        secs = by_group.get(group, [])
        if not secs:
            continue
        label = group_labels.get(group, group)
        inner = "\n".join(
            f'        <li><a href="#{s["section_id"]}">{s["finding"]}</a></li>'
            for s in secs
        )
        items.append(f"    <li><strong>{label}</strong>\n      <ul>\n{inner}\n      </ul>\n    </li>")

    # Adhoc spotlight: since every question now renders as its own <section id="{qid}">,
    # list each question as its own TOC entry (nested under the theme), rather than one
    # link to the spotlight as a whole — mirrors how regular sections are grouped above.
    adhoc_s = next((s for s in rendered_sections if s.get("section_id") == "adhoc_spotlight"), None)
    if adhoc_s:
        theme_label = adhoc_s.get("theme_label", "Special Focus")
        special_focus_label = _ui.get("adhoc_special_focus", "Special Focus")
        q_descs = adhoc_s.get("question_descriptions") or []
        if q_descs:
            def _toc_question_text(qd: dict) -> str:
                qt = re.sub(r"^[-–•]\s*", "", qd.get("question_text", "")).strip()
                return _clean_question_text(qt) if qt else ""

            inner = "\n".join(
                f'        <li><a href="#{qd["question_id"]}">{qd["question_id"].upper()}'
                f'{" — " + _toc_question_text(qd) if _toc_question_text(qd) else ""}</a></li>'
                for qd in q_descs
            )
            items.append(
                f'    <li><strong>⭐ {special_focus_label}: {theme_label}</strong>\n'
                f'      <ul>\n{inner}\n      </ul>\n    </li>'
            )
        else:
            items.append(
                f'    <li><a href="#adhoc_spotlight">⭐ {special_focus_label}: {theme_label}</a></li>'
            )

    if not items:
        return ""
    rows = "\n".join(items)
    return textwrap.dedent(f"""
<nav id="toc">
  <p class="toc-title">{toc_title}</p>
  <ul>
{rows}
  </ul>
</nav>
""").strip()


def build_html(
    rendered_sections: list[dict],
    annex_html: str,
    exec_bullets: list[dict],
    toc_html: str,
    painting_inner_html: str = "",
    latest_wave: int = 0,
    ui: dict | None = None,
) -> str:
    _ui = ui or {}
    today = date.today().strftime("%d %b %Y")
    wave_str = str(latest_wave)

    group_labels = {
        "Financing Conditions":        _ui.get("group_financing", "Financing Conditions"),
        "Economic Situation of Firms": _ui.get("group_economic",  "Economic Situation of Firms"),
    }
    fn_routed  = _ui.get("footnote_routed",   ROUTED_FOOTNOTE)
    fn_missing = _ui.get("footnote_missing",  MISSINGNESS_FOOTNOTE)
    fn_agentic = _ui.get("footnote_agentic",  _AGENTIC_FOOTNOTE)

    adhoc_s = next((s for s in rendered_sections if s.get("section_id") == "adhoc_spotlight"), None)
    regular_sections = [s for s in rendered_sections if s.get("section_id") != "adhoc_spotlight"]

    by_group: dict[str, list[dict]] = {}
    for s in regular_sections:
        g = s.get("group", "Other")
        by_group.setdefault(g, []).append(s)

    sections_parts = []
    for group in GROUP_ORDER:
        secs = by_group.get(group, [])
        if not secs:
            continue
        sections_parts.append(f"<h2>{group_labels.get(group, group)}</h2>")
        for s in secs:
            chart_b64 = base64.b64encode(s["chart_png"]).decode() if s.get("chart_png") else ""
            chart_html = (
                f'  <img class="chart-img" src="data:image/png;base64,{chart_b64}" '
                f'alt="{s["title"]} {_ui.get("chart_alt_suffix", "chart")}">\n'
            )
            footnote = (
                (fn_routed + "\n" if s.get("routed") else "") +
                (fn_missing + "\n" if s.get("has_missingness_caveat") else "") +
                (fn_agentic if s.get("tool_calls", 0) > 0 else "")
            )
            sections_parts.append(
                render_section(
                    section_id=s["section_id"],
                    headline=s["finding"],
                    subtitle=s["title"],
                    bullets=s["bullets"],
                    chart_html=chart_html,
                    footnote=footnote,
                )
            )

    if adhoc_s:
        special_focus_label = _ui.get("adhoc_special_focus", "Special Focus")
        read_more_label  = _ui.get("adhoc_read_more",    "Read more:")
        ecb_article_label = _ui.get("adhoc_ecb_article", "ECB Economic Bulletin focus article")
        ecb_link_html = ""
        if adhoc_s.get("ecb_article_url"):
            ecb_link_html = (
                f'  <p class="footnote">{read_more_label} '
                f'<a href="{adhoc_s["ecb_article_url"]}" target="_blank" rel="noopener">'
                f'{ecb_article_label}</a></p>\n'
            )
        theme_label = adhoc_s.get("theme_label", "Special Focus")

        if adhoc_s.get("sub_sections"):
            sub_section_parts = []
            for ss in adhoc_s["sub_sections"]:
                ss_chart_html = ""
                if ss.get("chart_png"):
                    ss_b64 = base64.b64encode(ss["chart_png"]).decode()
                    ss_chart_html = (
                        f'<img class="chart-img chart-img--adhoc" src="data:image/png;base64,{ss_b64}" '
                        f'alt="{ss["heading"]} {_ui.get("chart_alt_suffix", "chart")}">\n'
                    )
                ss_bullets = "\n".join(
                    f"    <li>{_md_to_html(b.lstrip('• ').strip())}</li>"
                    for b in ss.get("bullets", [])
                )
                sub_section_parts.append(textwrap.dedent(f"""
                    <div class="ai-sub-section">
                      <h3>{ss['heading']}</h3>
                      <p class="section-subtitle">{ss['finding']}</p>
                      {ss_chart_html}<ul>
                    {ss_bullets}
                      </ul>
                    </div>
                """).strip())
            spotlight_html = textwrap.dedent(f"""
                <section id="adhoc_spotlight" data-theme="{theme_label}">
                  <h2>{special_focus_label}: {theme_label}</h2>
                {"".join(sub_section_parts)}
                {ecb_link_html}</section>
            """).strip()
        else:
            # Per-question blocks: one heading + chart + bullets per selected question
            selected_qids = adhoc_s.get("selected_question_ids", [])
            bullets_by_q = adhoc_s.get("bullets_by_question", {})
            q_descs_by_id = {
                qd["question_id"]: qd
                for qd in (adhoc_s.get("question_descriptions") or [])
            }
            chart_pngs = adhoc_s.get("chart_pngs") or (
                [adhoc_s["chart_png"]] if adhoc_s.get("chart_png") else []
            )

            # If no per-question structure, fall back to flat bullets + all charts
            if not selected_qids or not bullets_by_q:
                flat_bullets_html = "\n".join(
                    f"    <li>{_md_to_html(b.lstrip('• ').strip())}</li>"
                    for b in adhoc_s.get("bullets", [])
                )
                all_charts_html = ""
                if chart_pngs:
                    img_tags = "".join(
                        f'<img class="chart-img chart-img--flex-third" src="data:image/png;base64,{base64.b64encode(png).decode()}" '
                        f'alt="{theme_label} {_ui.get("chart_alt_suffix", "chart")}">\n'
                        for png in chart_pngs
                    )
                    all_charts_html = (
                        f'<div style="display:flex;flex-wrap:wrap;gap:1rem;margin:10px 0 16px;">\n'
                        f'{img_tags}</div>\n'
                    )
                inner_html = (
                    f'    <h3>{adhoc_s["finding"]}</h3>\n'
                    f'    <p class="section-subtitle">{adhoc_s["title"]}</p>\n'
                    f'{all_charts_html}'
                    f'    <ul>\n{flat_bullets_html}\n    </ul>\n'
                )
            else:
                # Each question is its own independent <section>, matching the main
                # report's regular sections (SECTION_TMPL) exactly: story headline (h3),
                # question-text subtitle, bullets, then chart last — not nested inside
                # one big adhoc-spotlight wrapper. Every question in this wave's adhoc
                # module gets its own chart + bullets, not just a top-1-3 selection.
                q_sections = []
                for i, qid in enumerate(selected_qids):
                    qd = q_descs_by_id.get(qid, {})
                    qt = qd.get("question_text", "") or qid.upper()
                    qt = re.sub(r"^[-–•]\s*", "", qt).strip()
                    subtitle = f"{qid.upper()} — {qt}" if qt else qid.upper()
                    headline = qd.get("description", "").strip() or subtitle

                    chart_html = ""
                    if i < len(chart_pngs):
                        b64 = base64.b64encode(chart_pngs[i]).decode()
                        chart_html = (
                            f'<img class="chart-img chart-img--adhoc" src="data:image/png;base64,{b64}" '
                            f'alt="{qid} {_ui.get("chart_alt_suffix", "chart")}">\n'
                        )

                    q_bullets = bullets_by_q.get(qid, [])

                    q_sections.append(
                        render_section(
                            section_id=qid,
                            headline=_md_to_html(headline),
                            subtitle=subtitle,
                            bullets=q_bullets,
                            chart_html=chart_html,
                            section_class="adhoc-question-section",
                        )
                    )

                inner_html = (
                    f'    <p class="section-subtitle" style="margin-bottom:1rem;">'
                    f'{adhoc_s["finding"]}</p>\n'
                    + "\n".join(q_sections) + "\n"
                )

            spotlight_html = textwrap.dedent(f"""
                <div id="adhoc_spotlight" data-theme="{theme_label}">
                  <h2>{special_focus_label}: {theme_label}</h2>
                {inner_html}
                {ecb_link_html}</div>
            """).strip()

        # Collapsible "All adhoc questions" fallback — every question already gets its
        # own full subsection above (chart + bullets), so this only needs to render
        # questions that DIDN'T make it into that loop (e.g. chart build or bullet
        # write failed for that question) rather than duplicating every question.
        q_descs = adhoc_s.get("question_descriptions") or []
        rendered_qids = set(adhoc_s.get("selected_question_ids", []))
        q_descs = [qd for qd in q_descs if qd.get("question_id") not in rendered_qids]
        if q_descs:
            q_items = []
            for qd in q_descs:
                score = qd.get("interest_score", "")
                qtext = qd.get("question_text", "") or qd.get("question_id", "").upper()
                desc = qd.get("description", "")
                kf = qd.get("key_finding", "")
                interest_tmpl = _ui.get("interest_label", " (interest: {score}/5)")
                score_label = interest_tmpl.format(score=score) if isinstance(score, int) else ""
                key_finding_label = _ui.get("key_finding_label", "Key finding:")
                kf_html = f'<br><em>{key_finding_label} {_md_to_html(kf)}</em>' if kf else ""
                q_items.append(
                    f'<li><strong>{qd["question_id"].upper()}</strong>{score_label} '
                    f'— {qtext}<br>{_md_to_html(desc)}{kf_html}</li>'
                )
            all_q_label = _ui.get("adhoc_all_questions", "All adhoc questions")
            q_list_html = "<ul>" + "\n".join(q_items) + "</ul>"
            spotlight_html += (
                f'\n<details class="adhoc-all-questions" style="margin-top:0.5rem;">'
                f'<summary style="cursor:pointer;font-size:0.9em;color:#555;">'
                f'{all_q_label}</summary>'
                f'<div style="font-size:0.88em;padding:0.5rem 0.5rem 0;">{q_list_html}</div>'
                f'</details>'
            )

        sections_parts.append(spotlight_html)

    exec_h2 = _ui.get("exec_h2", "Executive Summary")
    painting_slot = (
        f'<div class="exec-painting">{painting_inner_html}</div>'
        if painting_inner_html else ""
    )
    exec_bullet_items = []
    for item in exec_bullets:
        if isinstance(item, dict):
            text = item.get("bullet", "").lstrip("• ").strip()
            sid = item.get("section_id", "").strip()
        else:
            text = str(item).lstrip("• ").strip()
            sid = ""
        if not text:
            continue
        text = _md_to_html(text)
        if sid:
            exec_bullet_items.append(f'    <li><a href="#{sid}">{text}</a></li>')
        else:
            exec_bullet_items.append(f"    <li>{text}</li>")
    exec_bullets_html = "\n".join(exec_bullet_items)
    exec_summary_div = (
        f'<div class="exec-summary" id="exec-summary">\n'
        f'  <h2>{exec_h2}</h2>\n'
        f'  <ul>\n{exec_bullets_html}\n  </ul>\n'
        f'</div>'
    ) if exec_bullets else ""
    exec_flex = (
        f'<div class="exec-flex">{painting_slot}{exec_summary_div}</div>'
        if (painting_slot or exec_summary_div) else ""
    )

    is_slovak = _ui.get("lang", "en") == "sk"
    lang_switch = (
        '<a class="lang-switch" href="index.html">🇬🇧 EN</a>\n'
        if is_slovak
        else '<a class="lang-switch" href="sk.html">🇸🇰 SK</a>\n'
    )

    return HTML_PAGE.format(
        lang=_ui.get("lang", "en"),
        lang_switch=lang_switch,
        title_str=_ui.get("title", "ECB SAFE Survey — Wave {wave} · Slovakia").format(wave=wave_str),
        h1_str=_ui.get("h1", "ECB SAFE Survey — Wave {wave} · Slovakia").format(wave=wave_str),
        meta_str=_ui.get("meta", "Slovakia · Euro Area · Germany &nbsp;|&nbsp; Generated {date}").format(date=today),
        footer_str=_ui.get(
            "footer",
            "Source: ECB SAFE microdata. Net balance = % reporting increase minus % reporting decrease. "
            "Positive = tightening/rising (adverse for firms unless noted). Negative = easing/falling."
        ),
        annex=annex_html,
        exec_flex=exec_flex,
        toc=toc_html,
        sections="\n\n".join(sections_parts),
    )
