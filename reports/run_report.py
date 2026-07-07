"""
SAFE Survey Report Generator — standard sections only.

Loops over SECTIONS in config.py:
  1. Fetch data for all sections via MotherDuck
  2. Parallel interest checks — returns interesting flag, chart_type, best_panel
  3. For interesting sections: build chart + generate Sonnet bullets
  4. Generate executive summary from all rendered sections
  5. Build collapsible question annex from MotherDuck ref_safe__annex
  6. Assemble single multi-section HTML report

Adhoc spotlight is handled separately by run_adhoc_report.py.

Usage:
  python run_report.py                # latest wave
  python run_report.py --wave 37      # retrospective report capped at wave 37

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token
  ANTHROPIC_API_KEY  — Anthropic API key
  MISTRAL_API_KEY    — Mistral API key
"""

import argparse
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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

from config import SECTIONS  # noqa: E402

from charts import CHART_STRINGS, SK_LABELS, build_chart, build_financing_gap_chart
from cost import _mistral_client
from db import PROD_SCHEMA, _get_connection, build_mart_catalogue, fetch_all
from html_builder import (
    _SK_UI, _fetch_painting_inner_html, build_annex_html, build_html, build_toc,
    _load_annex_question_texts,
)
from llm import (
    _add_so_what, _fetch_ecb_context, _sharpen_with_ecb,
    _write_wave_memory, build_section_signals, check_all_interest, classify_ecb_emphasis,
    get_exec_summary, get_section_content_agentic, get_shortened_questions, translate_to_slovak,
)

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
    parser.add_argument("--refresh-chart-titles", action="store_true",
                        help="Force regeneration of cached chart question captions")
    parser.add_argument("--no-cache", action="store_true",
                        help="Ignore section cache — regenerate all sections from scratch")
    parser.add_argument("--rerun-sections", type=str, default=None,
                        help="Comma-separated section IDs to regenerate (ignore cache for these only)")
    args = parser.parse_args()
    _force_rerun = set(s.strip() for s in args.rerun_sections.split(",")) if args.rerun_sections else set()

    print("Fetching data for all sections...")
    data = fetch_all()

    if args.wave is not None:
        print(f"  [RETROSPECTIVE] Capping data at wave {args.wave}")
        data = {sid: df[df["wave_number"] <= args.wave].copy() for sid, df in data.items()}

    for sid, df in data.items():
        print(f"  {sid}: {len(df)} rows")
    latest_wave = int(max(df["wave_number"].max() for df in data.values()))
    print(f"  Latest wave: {latest_wave}")

    cost_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    anthropic_client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    mistral_client = _mistral_client()
    cost_lock = threading.Lock()

    schema = PROD_SCHEMA
    tool_con = _get_connection()

    rendered: list[dict] = []
    ecb_context = ""
    historical_context = ""

    print("Running interest checks (parallel)...")
    interest = check_all_interest(SECTIONS, data, cost_tracker)
    for sid, r in interest.items():
        flag = "✓" if r["interesting"] else "✗"
        print(f"  {flag} {sid}: {r['reason']} [chart={r['chart_type']}, best_panel={r['best_panel']}]")

    try:
        rows = tool_con.execute("""
            SELECT wave_number, notable_summary
            FROM main_safe.ref_safe__wave_memory
            ORDER BY wave_number DESC LIMIT 3
        """).fetchall()
        if rows:
            historical_context = (
                "\n\n## Historical context (prior waves — for trend awareness only)\n"
                + "\n".join(f"  Wave {r[0]}: {r[1]}" for r in rows)
            )
    except Exception:
        pass
    interp_path = Path(__file__).parent / "output" / "interpretation_context.md"
    if interp_path.exists():
        interp_text = interp_path.read_text().strip()
        if interp_text:
            historical_context += (
                "\n\n## Interpretation notes from prior gap analysis\n" + interp_text
            )
    if historical_context:
        print(f"  Loaded historical context ({len(historical_context)} chars)")

    print("Fetching ECB publication for sharpener pass...")
    _, ecb_context = _fetch_ecb_context()
    if ecb_context:
        print(f"  Fetched {len(ecb_context):,} chars")
    else:
        print("  ECB fetch failed or unavailable — sharpener pass skipped")

    print("Building mart schema catalogue...")
    mart_catalogue = build_mart_catalogue(tool_con, schema)

    print("Loading annex question texts...")
    question_texts = _load_annex_question_texts(con=tool_con)
    print(f"  Loaded {len(question_texts)} question texts from annex")

    print("Loading/refreshing chart question captions...")
    chart_question_captions = get_shortened_questions(
        SECTIONS, question_texts, tool_con, schema, mistral_client, cost_tracker,
        force_refresh=args.refresh_chart_titles,
    )
    print(f"  {len(chart_question_captions)} section captions ready")

    interesting_sections = [s for s in SECTIONS if interest[s["id"]]["interesting"]]
    skipped = [s["id"] for s in SECTIONS if not interest[s["id"]]["interesting"]]
    for sid in skipped:
        print(f"  Skipping {sid} (not interesting)")

    _cache_dir = OUTPUT_DIR / "section_cache"
    _cache_dir.mkdir(exist_ok=True)

    def _build_section(sec: dict) -> dict:
        sid = sec["id"]
        r = interest[sid]

        with cost_lock:
            _check_cost_ceiling(cost_tracker)

        # ── Cache read ────────────────────────────────────────────────────────
        cache_path = _cache_dir / f"{sid}_w{latest_wave}.json"
        use_cache = (
            not args.no_cache
            and sid not in _force_rerun
            and cache_path.exists()
        )
        if use_cache:
            cached = json.loads(cache_path.read_text())
            if cached.get("wave_number") == latest_wave:
                print(f"  [CACHE HIT] {sid} — rebuilding chart only")
                chart_title = cached["finding"]
                chart_question = chart_question_captions.get(sid, "")
                chart_subtitle = cached.get("chart_subtitle", "")
                if sid == "financing_gap":
                    chart_png = build_financing_gap_chart(sec, data[sid],
                                                          chart_title=chart_title, chart_question=chart_question)
                elif sid == "bank_loan_terms":
                    chart_png = build_chart(sec, data[sid], "bar", r["best_panel"],
                                            chart_subtitle=chart_subtitle,
                                            chart_title=chart_title, chart_question=chart_question,
                                            panel_title_suffix=CHART_STRINGS["net_change_pct_suffix"],
                                            pct_axis=True)
                else:
                    chart_png = build_chart(sec, data[sid], r["chart_type"], r["best_panel"],
                                            chart_subtitle=chart_subtitle,
                                            chart_title=chart_title, chart_question=chart_question)
                return {
                    "section_id": sid,
                    "title": sec["title"],
                    "group": sec.get("group", "Other"),
                    "finding": cached["finding"],
                    "bullets": cached["bullets"],
                    "chart_png": chart_png,
                    "chart_subtitle": chart_subtitle,
                    "sign_note": sec["sign_note"],
                    "routed": sec.get("routed", False),
                    "has_missingness_caveat": sec.get("has_missingness_caveat", False),
                    "tool_calls": cached.get("tool_calls", 0),
                    "grounding_warnings": cached.get("grounding_warnings", []),
                }

        thread_con = _get_connection()
        try:
            local_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}
            print(f"  Generating finding + bullets for {sid}...")
            content = get_section_content_agentic(
                sec, data[sid], thread_con, schema, mart_catalogue,
                local_tracker, question_texts, client=anthropic_client,
                historical_context=historical_context,
            )

            print(f"  Adding implications for {sid}...")
            content = _add_so_what(content, sec, mistral_client, local_tracker)

            with cost_lock:
                cost_tracker["input_tokens"] += local_tracker["input_tokens"]
                cost_tracker["output_tokens"] += local_tracker["output_tokens"]
                cost_tracker["usd"] += local_tracker["usd"]
                cost_tracker["calls"] += local_tracker["calls"]
                for model, m in local_tracker["by_model"].items():
                    bm = cost_tracker["by_model"].setdefault(model, {"calls": 0, "input": 0, "output": 0, "usd": 0.0, "cache_write": 0, "cache_read": 0})
                    for k in bm:
                        bm[k] += m.get(k, 0)

            print(f"    [{sid}] finding: {content['finding']}")
            for b in content["bullets"]:
                print(f"    [{sid}] {b}")

            chart_subtitle = content.get("chart_subtitle", "")
            chart_title = content["finding"]
            chart_question = chart_question_captions.get(sid, "")
            print(f"  Building chart for {sid}...")
            if sid == "financing_gap":
                chart_png = build_financing_gap_chart(sec, data[sid],
                                                      chart_title=chart_title, chart_question=chart_question)
            elif sid == "bank_loan_terms":
                chart_png = build_chart(sec, data[sid], "bar", r["best_panel"],
                                        chart_subtitle=chart_subtitle,
                                        chart_title=chart_title, chart_question=chart_question,
                                        panel_title_suffix=CHART_STRINGS["net_change_pct_suffix"],
                                        pct_axis=True)
            else:
                chart_png = build_chart(sec, data[sid], r["chart_type"], r["best_panel"],
                                        chart_subtitle=chart_subtitle,
                                        chart_title=chart_title, chart_question=chart_question)

            result = {
                "section_id": sid,
                "title": sec["title"],
                "group": sec.get("group", "Other"),
                "finding": content["finding"],
                "bullets": content["bullets"],
                "chart_png": chart_png,
                "chart_subtitle": chart_subtitle,
                "sign_note": sec["sign_note"],
                "routed": sec.get("routed", False),
                "has_missingness_caveat": sec.get("has_missingness_caveat", False),
                "tool_calls": content.get("tool_calls", 0),
                "grounding_warnings": content.get("grounding_warnings", []),
            }

            # ── Cache write ───────────────────────────────────────────────────
            cache_path.write_text(json.dumps({
                "section_id": sid,
                "wave_number": latest_wave,
                "finding": content["finding"],
                "bullets": content["bullets"],
                "chart_subtitle": content.get("chart_subtitle", ""),
                "grounding_warnings": content.get("grounding_warnings", []),
                "tool_calls": content.get("tool_calls", 0),
            }, indent=2, ensure_ascii=False))

            return result
        finally:
            thread_con.close()

    print(f"Generating {len(interesting_sections)} sections (parallel)...")
    rendered_map: dict[str, dict] = {}
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_build_section, sec): sec["id"] for sec in interesting_sections}
        for future in as_completed(futures):
            result = future.result()
            rendered_map[result["section_id"]] = result

    rendered = [rendered_map[s["id"]] for s in SECTIONS if s["id"] in rendered_map]

    _check_cost_ceiling(cost_tracker)

    if ecb_context:
        print("Sharpening bullets against ECB publication...")
        rendered = _sharpen_with_ecb(rendered, ecb_context, mistral_client, cost_tracker)

    _annex_loaded_ok = len(question_texts) > 0

    print("Classifying ECB emphasis + computing exec-summary signals...")
    sections_by_id = {s["id"]: s for s in SECTIONS}
    ecb_emphasis = classify_ecb_emphasis(ecb_context, interesting_sections, mistral_client, cost_tracker)
    section_signals = build_section_signals(rendered, data, sections_by_id, ecb_emphasis)

    print("Generating executive summary (two-pass)...")
    exec_bullets = get_exec_summary(
        rendered, cost_tracker, historical_context=historical_context,
        section_signals=section_signals, anthropic_client=anthropic_client,
    ) if rendered else []
    for item in exec_bullets:
        print(f"  [{item.get('section_id', '?')}] {item.get('bullet', '')}")

    if exec_bullets and rendered:
        print("Writing wave memory...")
        _write_wave_memory(latest_wave, exec_bullets, rendered,
                           mistral_client, tool_con, cost_tracker)

    print("Building TOC...")
    toc_html = build_toc(rendered)

    print("Building question annex...")
    annex_html = build_annex_html(con=tool_con)

    print("Fetching painting thumbnail...")
    painting_inner_html = _fetch_painting_inner_html()

    print("Assembling HTML (EN)...")
    html = build_html(rendered, annex_html, exec_bullets, toc_html, painting_inner_html, latest_wave)

    # Always write both the wave-numbered archive copy and the _latest alias.
    wave_en = f"report_q{latest_wave}.html"
    wave_sk = f"report_q{latest_wave}_sk.html"
    (OUTPUT_DIR / wave_en).write_text(html, encoding="utf-8")
    (OUTPUT_DIR / "report_latest.html").write_text(html, encoding="utf-8")
    print(f"Saved → {OUTPUT_DIR / wave_en}")
    print(f"WAVE_REPORT_EN={wave_en}")

    print("Translating to Slovak...")
    sk_rendered, sk_exec_bullets, sk_question_texts = translate_to_slovak(
        rendered, exec_bullets, cost_tracker, question_texts=question_texts,
    )
    sk_annex_html = build_annex_html(con=tool_con, ui=_SK_UI, question_texts_override=sk_question_texts)

    _next_release_date = None
    _next_release_note = None
    try:
        _cal_row = tool_con.execute("""
            SELECT next_release_date, reference_period
            FROM main_safe.ref_safe__release_calendar
            WHERE dataset = 'SAFE'
        """).fetchone()
        if _cal_row and _cal_row[0]:
            _next_release_date = _cal_row[0].isoformat()
            _next_release_note = _cal_row[1] or None
    except Exception as e:
        print(f"  WARNING: could not read release calendar: {e}")

    tool_con.close()

    print("Rebuilding charts with Slovak labels...")
    for sk_sec in sk_rendered:
        sid = sk_sec.get("section_id")
        if sid not in data or sid not in sections_by_id:
            continue
        r = interest[sid]
        sec = sections_by_id[sid]
        chart_title = sk_sec["finding"]
        chart_question = chart_question_captions.get(sid, "")
        chart_subtitle = sk_sec.get("chart_subtitle", "")
        try:
            if sid == "financing_gap":
                sk_sec["chart_png"] = build_financing_gap_chart(
                    sec, data[sid], chart_title=chart_title, chart_question=chart_question,
                    labels=SK_LABELS,
                )
            elif sid == "bank_loan_terms":
                sk_sec["chart_png"] = build_chart(
                    sec, data[sid], "bar", r["best_panel"], chart_subtitle=chart_subtitle,
                    chart_title=chart_title, chart_question=chart_question, labels=SK_LABELS,
                    panel_title_suffix=SK_LABELS["strings"]["net_change_pct_suffix"],
                    pct_axis=True,
                )
            else:
                sk_sec["chart_png"] = build_chart(
                    sec, data[sid], r["chart_type"], r["best_panel"], chart_subtitle=chart_subtitle,
                    chart_title=chart_title, chart_question=chart_question, labels=SK_LABELS,
                )
        except Exception as e:
            print(f"  SK chart rebuild failed for {sid} — keeping English chart: {e}")

    sk_toc_html = build_toc(sk_rendered, ui=_SK_UI)
    sk_html = build_html(sk_rendered, sk_annex_html, sk_exec_bullets, sk_toc_html,
                         painting_inner_html, latest_wave, ui=_SK_UI)
    (OUTPUT_DIR / wave_sk).write_text(sk_html, encoding="utf-8")
    (OUTPUT_DIR / "report_latest_sk.html").write_text(sk_html, encoding="utf-8")
    print(f"Saved → {OUTPUT_DIR / wave_sk}")
    print(f"WAVE_REPORT_SK={wave_sk}")

    from datetime import date as _date

    _pages_base = "https://lubospernis.github.io/safe_ai"
    _links = {
        "wave": latest_wave,
        "en": f"{_pages_base}/{wave_en}",
        "sk": f"{_pages_base}/{wave_sk}",
        "last_updated": _date.today().isoformat(),
    }
    if _next_release_date:
        _links["next_release"] = _next_release_date
        if _next_release_note:
            _links["next_release_note"] = _next_release_note

    (OUTPUT_DIR / "latest_links.json").write_text(
        json.dumps(_links, indent=2), encoding="utf-8"
    )
    print(f"Links → {_links['en']}")

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

    cache_read_total = sum(m.get("cache_read", 0) for m in cost_tracker["by_model"].values())
    _now = _dt.utcnow()
    # Derive canonical model labels from what was actually used
    _anthropic_models = sorted(m for m in cost_tracker["by_model"] if "claude" in m)
    _mistral_models = sorted(m for m in cost_tracker["by_model"] if "mistral" in m)
    _model_sonnet = _anthropic_models[0] if _anthropic_models else "claude-sonnet-4-6"
    _model_mistral = _mistral_models[0] if _mistral_models else "mistral-small-latest"
    _run_snapshot = {
        "run_type": "main",
        "run_date": _now.strftime("%Y-%m-%d"),
        "run_time": _now.strftime("%H:%M:%S"),
        "sections_summary": [
            {
                "section_id": s["section_id"],
                "finding": s.get("finding", ""),
                "bullets": s.get("bullets", []),
                "grounding_warnings": s.get("grounding_warnings", []),
            }
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
        "n_sections": len(rendered),
        "grounding_warning_count": sum(len(s.get("grounding_warnings", [])) for s in rendered),
        "duration_seconds": round((_dt.now() - _run_start).total_seconds(), 1),
        "context_sources": {
            "annex_loaded": _annex_loaded_ok,
        },
    }
    # Append to run_log.json (array of all runs; last entry = this run)
    _run_log_path = OUTPUT_DIR / "run_log.json"
    _existing_log: list = json.loads(_run_log_path.read_text()) if _run_log_path.exists() else []
    _existing_log.append(_run_snapshot)
    _run_log_path.write_text(json.dumps(_existing_log, indent=2, ensure_ascii=False))
    print("Done.")


if __name__ == "__main__":
    main()
