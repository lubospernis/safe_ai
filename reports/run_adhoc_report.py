"""
SAFE Survey Adhoc Report Generator — adhoc spotlight only.

Detects whether the latest wave has an adhoc module (by querying mart_safe__adhoc_responses).
If no adhoc data is present, exits 0 without producing any output — safe to run every wave.
When adhoc data is found, runs the full 5-phase agentic pipeline and produces:
  - report_adhoc_latest.html  (EN)
  - report_adhoc_latest_sk.html  (SK)
  - run_adhoc_log.json  (appended, same format as run_log.json)

Standard section content is handled separately by run_report.py.

Usage:
  python run_adhoc_report.py           # latest wave
  python run_adhoc_report.py --wave 37 # retrospective adhoc report for wave 37

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token (prod only)
  ANTHROPIC_API_KEY  — Anthropic API key
  MISTRAL_API_KEY    — Mistral API key
"""

import argparse
import json
import os
import sys
from pathlib import Path

import anthropic
import matplotlib

matplotlib.use("Agg")

# NBS brand rcParams must be set before any chart module imports.
# DejaVu Sans (matplotlib's built-in default) is used instead of Arial — Arial isn't
# installed on GitHub Actions runners, which silently fell back to DejaVu Sans there
# anyway (with a "findfont: Font family 'Arial' not found" warning); this makes local
# and CI-rendered charts consistent instead of differing by environment.
matplotlib.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 9,
    "axes.labelsize": 8,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "legend.fontsize": 9,
    "figure.facecolor": "#f4f4f4",
    "axes.facecolor": "#f4f4f4",
})

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from adhoc import (
    build_adhoc_spotlight, detect_adhoc_theme, rebuild_adhoc_charts_en, rebuild_adhoc_charts_sk,
)
from cost import _mistral_client
from db import PROD_SCHEMA, _get_connection, fetch_all
from html_builder import (
    _SK_UI, _fetch_painting_inner_html, _load_annex_question_texts, build_annex_html, build_html, build_toc,
)
from llm import _find_ecb_focus_article, get_exec_summary, translate_to_slovak

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

# Abort the run if spend crosses this ceiling — guards against a runaway loop or
# pricing-table error silently burning API budget. Override via env for testing.
COST_CEILING_USD = float(os.environ.get("COST_CEILING_USD", "15.0"))


class CostCeilingExceeded(RuntimeError):
    pass


def _check_cost_ceiling(cost_tracker: dict) -> None:
    if cost_tracker["usd"] > COST_CEILING_USD:
        raise CostCeilingExceeded(
            f"Spend ${cost_tracker['usd']:.2f} exceeded ceiling ${COST_CEILING_USD:.2f} — aborting run"
        )


def main() -> None:
    from datetime import datetime as _dt
    _run_start = _dt.now()

    parser = argparse.ArgumentParser()
    parser.add_argument("--wave", type=int, default=None,
                        help="Cap data at this wave number for retrospective reports (e.g. --wave 37)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignore cached adhoc spotlight and regenerate from scratch")
    parser.add_argument("--rerun-sections", type=str, default=None,
                        help="Comma-separated section IDs to force-rerun (e.g. adhoc_spotlight)")
    parser.add_argument("--rebuild-charts-only", action="store_true",
                        help="Dev flag: on cache hit, only re-run chart building (Phase 2) from the "
                             "cached question data — skips every LLM phase (descriptions, bullets, "
                             "review, exec summary, translation). For verifying chart/label formatting "
                             "changes cheaply. Requires a cache entry from a prior full run; falls back "
                             "to a full run if none exists. Note: bullets were written against the OLD "
                             "charts in this mode, so it is for dev iteration only, not final output.")
    args = parser.parse_args()
    _force_rerun = set(s.strip() for s in args.rerun_sections.split(",")) if args.rerun_sections else set()

    # We still need wave context — fetch a minimal dataset to find latest_wave
    print("Fetching data to determine latest wave...")
    data = fetch_all()
    if args.wave is not None:
        print(f"  [RETROSPECTIVE] Capping data at wave {args.wave}")
        data = {sid: df[df["wave_number"] <= args.wave].copy() for sid, df in data.items()}
    latest_wave = int(max(df["wave_number"].max() for df in data.values()))
    print(f"  Latest wave: {latest_wave}")

    cost_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    mistral_client = _mistral_client()

    schema = PROD_SCHEMA
    tool_con = _get_connection()

    # ── Detect adhoc theme ────────────────────────────────────────────────────
    print("Detecting adhoc module...")
    adhoc_theme = detect_adhoc_theme(latest_wave, tool_con, schema, mistral_client, cost_tracker)
    if not adhoc_theme:
        print("No adhoc data for this wave — skipping.")
        tool_con.close()
        sys.exit(0)

    _adhoc_module = adhoc_theme["module_id"]
    _adhoc_topic = adhoc_theme["theme_label"]
    _questionnaire_url = adhoc_theme.get("questionnaire_url")
    _questionnaire_labels_ok = bool(adhoc_theme.get("response_labels"))
    print(f"  Adhoc theme detected: {adhoc_theme['theme_label']} ({adhoc_theme['module_id']})")
    if not _questionnaire_url:
        print("  WARN: No questionnaire URL resolved — response code labels unavailable")
    elif not _questionnaire_labels_ok:
        print(f"  WARN: Questionnaire fetched ({_questionnaire_url}) but no answer labels parsed")

    # ── Build adhoc spotlight (with section cache) ───────────────────────────
    # Cache is pickled (not JSON) so it can hold the raw DataFrames each question's
    # chart was built from (_chart_rebuild_specs) — needed for --rebuild-charts-only
    # to re-run just Phase 2 (chart building) without re-paying for every LLM phase.
    import pickle
    _cache_dir = OUTPUT_DIR / "section_cache"
    _cache_dir.mkdir(exist_ok=True)
    _cache_path = _cache_dir / f"adhoc_spotlight_w{latest_wave}.pkl"
    _sid = "adhoc_spotlight"
    _use_cache = (
        not args.no_cache
        and _sid not in _force_rerun
        and _cache_path.exists()
    )
    adhoc_section = None
    if _use_cache:
        _cached = pickle.loads(_cache_path.read_bytes())
        if _cached.get("wave_number") == latest_wave:
            adhoc_section = _cached
        else:
            _use_cache = False

    if _use_cache and args.rebuild_charts_only:
        _rebuild_specs = adhoc_section.get("_chart_rebuild_specs")
        if _rebuild_specs:
            print(f"  [CACHE HIT] {_sid} — rebuilding charts only (Phase 2), skipping all LLM phases")
            new_pngs = rebuild_adhoc_charts_en(
                _rebuild_specs, adhoc_section.get("_response_labels", {}), mistral_client, cost_tracker,
                theme_label=adhoc_section.get("theme_label", ""),
            )
            adhoc_section["chart_pngs"] = new_pngs
            adhoc_section["chart_png"] = new_pngs[0] if new_pngs else None
        else:
            print("  --rebuild-charts-only requested but cache has no chart_rebuild_specs — "
                  "falling back to a full rebuild")
            _use_cache = False
            adhoc_section = None
    elif _use_cache:
        print(f"  [CACHE HIT] {_sid} — skipping LLM phases")

    if not _use_cache:
        adhoc_section = build_adhoc_spotlight(
            adhoc_theme, latest_wave, tool_con, schema, mistral_client, cost_tracker,
            anthropic_client=anthropic_client,
        )
        if adhoc_section:
            _cache_entry = dict(adhoc_section)
            _cache_entry["wave_number"] = latest_wave
            _cache_path.write_bytes(pickle.dumps(_cache_entry))

    if not adhoc_section:
        print("Adhoc spotlight build failed — no output produced.")
        tool_con.close()
        sys.exit(1)

    _check_cost_ceiling(cost_tracker)

    print(f"  Adhoc spotlight generated: {adhoc_section['finding']}")
    if not adhoc_section.get("review_passed", True):
        print("  WARNING: Adhoc review scored < 8 on at least one dimension — investigate before publishing")

    adhoc_ecb_url = _find_ecb_focus_article(
        adhoc_theme["theme_label"], mistral_client, cost_tracker
    )
    if adhoc_ecb_url:
        adhoc_section["ecb_article_url"] = adhoc_ecb_url
        print(f"  ECB focus article: {adhoc_ecb_url}")

    rendered = [adhoc_section]

    # ── Translation cache (dev-only): reused verbatim in --rebuild-charts-only
    # mode so that mode skips the exec-summary + Slovak-translation LLM calls too,
    # not just Phase 1-4. Charts (EN + SK) are still rebuilt fresh either way.
    _translation_cache_path = _cache_dir / f"adhoc_translation_w{latest_wave}.pkl"
    _translation_cached = None
    if args.rebuild_charts_only and _use_cache and _translation_cache_path.exists():
        _translation_cached = pickle.loads(_translation_cache_path.read_bytes())

    # ── Executive summary (adhoc-only) ───────────────────────────────────────
    if _translation_cached:
        print("  [CACHE HIT] executive summary — reusing cached bullets")
        exec_bullets = _translation_cached["exec_bullets"]
    else:
        print("Generating executive summary...")
        exec_bullets = get_exec_summary(
            rendered, cost_tracker, adhoc_section=adhoc_section, anthropic_client=anthropic_client
        ) if adhoc_section else []
    for item in exec_bullets:
        print(f"  [{item.get('section_id', '?')}] {item.get('bullet', '')}")

    # ── Assemble HTML ────────────────────────────────────────────────────────
    print("Building TOC...")
    toc_html = build_toc(rendered)

    print("Building question annex...")
    annex_html = build_annex_html(con=tool_con)
    question_texts = _load_annex_question_texts(con=tool_con)

    print("Fetching painting thumbnail...")
    painting_inner_html = _fetch_painting_inner_html()

    print("Assembling HTML (EN)...")
    html = build_html(rendered, annex_html, exec_bullets, toc_html, painting_inner_html, latest_wave)

    wave_en = f"report_adhoc_q{latest_wave}.html"
    wave_sk = f"report_adhoc_q{latest_wave}_sk.html"
    (OUTPUT_DIR / wave_en).write_text(html, encoding="utf-8")
    (OUTPUT_DIR / "report_adhoc_latest.html").write_text(html, encoding="utf-8")
    print(f"Saved → {OUTPUT_DIR / wave_en}")
    print(f"WAVE_ADHOC_EN={wave_en}")

    if _translation_cached:
        print("  [CACHE HIT] Slovak translation — reusing cached text")
        sk_rendered = _translation_cached["sk_rendered"]
        sk_exec_bullets = _translation_cached["sk_exec_bullets"]
        sk_question_texts = _translation_cached["sk_question_texts"]
    else:
        print("Translating to Slovak...")
        sk_rendered, sk_exec_bullets, sk_question_texts = translate_to_slovak(
            rendered, exec_bullets, cost_tracker, question_texts=question_texts,
        )
        _translation_cache_path.write_bytes(pickle.dumps({
            "sk_rendered": sk_rendered,
            "sk_exec_bullets": sk_exec_bullets,
            "sk_question_texts": sk_question_texts,
            "exec_bullets": exec_bullets,
        }))
    sk_annex_html = build_annex_html(con=tool_con, ui=_SK_UI, question_texts_override=sk_question_texts)
    tool_con.close()

    _rebuild_specs = adhoc_section.get("_chart_rebuild_specs")
    if _rebuild_specs:
        print("Rebuilding adhoc charts with Slovak labels...")
        sk_adhoc = sk_rendered[0]
        sk_chart_pngs = rebuild_adhoc_charts_sk(
            _rebuild_specs, sk_question_texts, adhoc_section.get("_response_labels", {}),
            theme_label_sk=sk_adhoc.get("theme_label", ""),
        )
        sk_adhoc["chart_pngs"] = sk_chart_pngs
        sk_adhoc["chart_png"] = sk_chart_pngs[0] if sk_chart_pngs else None
    else:
        print("Skipping SK chart rebuild — adhoc section was loaded from cache (no rebuild data).")

    sk_toc_html = build_toc(sk_rendered, ui=_SK_UI)
    sk_html = build_html(sk_rendered, sk_annex_html, sk_exec_bullets, sk_toc_html,
                         painting_inner_html, latest_wave, ui=_SK_UI)
    (OUTPUT_DIR / wave_sk).write_text(sk_html, encoding="utf-8")
    (OUTPUT_DIR / "report_adhoc_latest_sk.html").write_text(sk_html, encoding="utf-8")
    print(f"Saved → {OUTPUT_DIR / wave_sk}")
    print(f"WAVE_ADHOC_SK={wave_sk}")

    _pages_base = "https://lubospernis.github.io/safe_ai"
    _links = {
        "wave": latest_wave,
        "en": f"{_pages_base}/{wave_en}",
        "sk": f"{_pages_base}/{wave_sk}",
    }
    (OUTPUT_DIR / "latest_adhoc_links.json").write_text(
        json.dumps(_links, indent=2), encoding="utf-8"
    )
    print(f"Links → {_links['en']}")

    # ── Cost summary ─────────────────────────────────────────────────────────
    w = 54
    print(f"\n{'─' * w}\nRun cost estimate")
    for model, m in sorted(cost_tracker["by_model"].items()):
        cache_note = ""
        if m.get("cache_write") or m.get("cache_read"):
            cache_note = f"  (cache write={m['cache_write']:,} read={m['cache_read']:,})"
        print(f"  {model:<28} {m['calls']:>3} calls  "
              f"{m['input']:>7,} in  {m['output']:>5,} out  ${m['usd']:.4f}{cache_note}")
    print(f"  {'─' * (w - 2)}")
    print(f"  {'Total':<28} {cost_tracker['calls']:>3} calls  "
          f"{cost_tracker['input_tokens']:>7,} in  {cost_tracker['output_tokens']:>5,} out  "
          f"${cost_tracker['usd']:.4f}")
    print(f"{'─' * w}")

    # ── Write run log ─────────────────────────────────────────────────────────
    cache_read_total = sum(m.get("cache_read", 0) for m in cost_tracker["by_model"].values())
    _now = _dt.utcnow()
    _anthropic_models = sorted(m for m in cost_tracker["by_model"] if "claude" in m)
    _mistral_models = sorted(m for m in cost_tracker["by_model"] if "mistral" in m)
    _model_sonnet = _anthropic_models[0] if _anthropic_models else "claude-sonnet-4-6"
    _model_mistral = _mistral_models[0] if _mistral_models else "mistral-small-latest"
    _run_snapshot = {
        "run_type": "adhoc",
        "run_date": _now.strftime("%Y-%m-%d"),
        "run_time": _now.strftime("%H:%M:%S"),
        "sections_summary": [
            {"section_id": s["section_id"], "finding": s.get("finding", ""), "bullets": s.get("bullets", [])}
            for s in rendered
        ],
        "wave_number": latest_wave,
        "total_cost_usd": round(cost_tracker["usd"], 5),
        "input_tokens": cost_tracker["input_tokens"],
        "output_tokens": cost_tracker["output_tokens"],
        "cache_read_tokens": cache_read_total,
        "model_sonnet": _model_sonnet,
        "model_mistral": _model_mistral,
        "cost_by_model": {
            model: {
                "calls": m["calls"],
                "input_tokens": m["input"],
                "output_tokens": m["output"],
                "usd": round(m["usd"], 5),
            }
            for model, m in sorted(cost_tracker["by_model"].items())
        },
        "n_sections": 1,
        "duration_seconds": round((_dt.now() - _run_start).total_seconds(), 1),
        "context_sources": {
            "adhoc_module": _adhoc_module,
            "adhoc_topic": _adhoc_topic,
            "questionnaire_url": _questionnaire_url,
            "questionnaire_labels_parsed": _questionnaire_labels_ok,
        },
        "grounding_warning_count": len(adhoc_section.get("grounding_warnings", [])),
        "review_scores": adhoc_section.get("review_scores", {}),
        "review_passed": adhoc_section.get("review_passed", True),
    }
    _run_log_path = OUTPUT_DIR / "run_adhoc_log.json"
    _existing_log: list = json.loads(_run_log_path.read_text()) if _run_log_path.exists() else []
    _existing_log.append(_run_snapshot)
    _run_log_path.write_text(json.dumps(_existing_log, indent=2, ensure_ascii=False))
    print("Done.")


if __name__ == "__main__":
    main()
