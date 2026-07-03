"""
SAFE Survey Report Generator — config-driven multi-section orchestrator.

Loops over SECTIONS in config.py:
  1. Fetch data for all sections via MotherDuck (prod) or local dev.duckdb (dev)
  2. Parallel interest checks — returns interesting flag, chart_type, best_panel
  3. For interesting sections: build chart + generate Sonnet bullets
  4. Generate executive summary from all rendered sections
  5. Build collapsible question annex from MotherDuck ref_safe__annex
  6. Assemble single multi-section HTML report

Usage:
  python run_report.py           # prod (MotherDuck, requires MOTHERDUCK_TOKEN)
  python run_report.py --dev     # local dev.duckdb, no token needed

Required environment variables:
  MOTHERDUCK_TOKEN   — MotherDuck service token (prod only)
  ANTHROPIC_API_KEY  — Anthropic API key
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

# NBS brand rcParams must be set before any chart module imports
matplotlib.rcParams.update({
    "font.family": "Arial",
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

from adhoc import build_adhoc_spotlight, detect_adhoc_theme
from charts import build_chart, build_financing_gap_chart
from cost import _mistral_client
from db import DEV_SCHEMA, PROD_SCHEMA, _get_connection, build_mart_catalogue, fetch_all
from html_builder import (
    _SK_UI, _fetch_painting_inner_html, build_annex_html, build_html, build_toc,
    _load_annex_question_texts,
)
from llm import (
    _add_so_what, _fetch_ecb_context, _find_ecb_focus_article, _sharpen_with_ecb,
    _write_wave_memory, check_all_interest, get_exec_summary, get_section_content_agentic,
    translate_to_slovak,
)

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def main() -> None:
    from datetime import datetime as _dt
    _run_start = _dt.now()

    parser = argparse.ArgumentParser()
    parser.add_argument("--dev", action="store_true",
                        help="Use local dev.duckdb instead of MotherDuck (no MOTHERDUCK_TOKEN needed)")
    parser.add_argument("--wave", type=int, default=None,
                        help="Cap data at this wave number for retrospective reports (e.g. --wave 37)")
    parser.add_argument("--adhoc-only", action="store_true",
                        help="Skip main sections; only build the adhoc spotlight (cheaper for adhoc iteration)")
    args = parser.parse_args()

    if args.dev:
        print(f"[DEV] Using local DuckDB")
    else:
        print("[PROD] Using MotherDuck")

    print("Fetching data for all sections...")
    data = fetch_all(dev=args.dev)

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

    schema = DEV_SCHEMA if args.dev else PROD_SCHEMA
    tool_con = _get_connection(args.dev)

    rendered: list[dict] = []
    ecb_context = ""
    historical_context = ""  # populated below for full runs only

    if not args.adhoc_only:
        print("Running interest checks (parallel)...")
        interest = check_all_interest(SECTIONS, data, cost_tracker)
        for sid, r in interest.items():
            flag = "✓" if r["interesting"] else "✗"
            print(f"  {flag} {sid}: {r['reason']} [chart={r['chart_type']}, best_panel={r['best_panel']}]")

        historical_context = ""
        if not args.dev:
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

        if not args.dev:
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

        interesting_sections = [s for s in SECTIONS if interest[s["id"]]["interesting"]]
        skipped = [s["id"] for s in SECTIONS if not interest[s["id"]]["interesting"]]
        for sid in skipped:
            print(f"  Skipping {sid} (not interesting)")

        def _build_section(sec: dict) -> dict:
            sid = sec["id"]
            r = interest[sid]
            thread_con = _get_connection(args.dev)
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
                print(f"  Building chart for {sid}...")
                if sid == "financing_gap":
                    chart_png = build_financing_gap_chart(sec, data[sid])
                elif sid == "bank_loan_terms":
                    chart_png = build_chart(sec, data[sid], "bar", r["best_panel"],
                                            chart_subtitle=chart_subtitle)
                else:
                    chart_png = build_chart(sec, data[sid], r["chart_type"], r["best_panel"],
                                            chart_subtitle=chart_subtitle)

                return {
                    "section_id": sid,
                    "title": sec["title"],
                    "group": sec.get("group", "Other"),
                    "finding": content["finding"],
                    "bullets": content["bullets"],
                    "chart_png": chart_png,
                    "sign_note": sec["sign_note"],
                    "routed": sec.get("routed", False),
                    "has_missingness_caveat": sec.get("has_missingness_caveat", False),
                    "tool_calls": content.get("tool_calls", 0),
                }
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

        if ecb_context:
            print("Sharpening bullets against ECB publication...")
            rendered = _sharpen_with_ecb(rendered, ecb_context, mistral_client, cost_tracker)
    else:
        print("[--adhoc-only] Skipping main sections, interest checks, and ECB sharpener.")

    adhoc_section: dict | None = None
    _adhoc_module: str | None = None
    _adhoc_topic: str | None = None
    _questionnaire_url: str | None = None
    _questionnaire_labels_ok: bool = False
    _annex_loaded_ok = (len(question_texts) > 0) if not args.adhoc_only else None

    print("Checking for adhoc module spotlight...")
    adhoc_theme = detect_adhoc_theme(latest_wave, tool_con, schema, mistral_client, cost_tracker)
    if adhoc_theme:
        _adhoc_module = adhoc_theme["module_id"]
        _adhoc_topic = adhoc_theme["theme_label"]
        _questionnaire_url = adhoc_theme.get("questionnaire_url")
        _questionnaire_labels_ok = bool(adhoc_theme.get("response_labels"))
        print(f"  Adhoc theme detected: {adhoc_theme['theme_label']} ({adhoc_theme['module_id']})")
        if not _questionnaire_url:
            print("  WARN: No questionnaire URL resolved — response code labels unavailable")
        elif not _questionnaire_labels_ok:
            print(f"  WARN: Questionnaire fetched ({_questionnaire_url}) but no answer labels parsed")
        adhoc_section = build_adhoc_spotlight(
            adhoc_theme, latest_wave, tool_con, schema, mistral_client, cost_tracker,
            anthropic_client=anthropic_client,
        )
        if adhoc_section:
            rendered.append(adhoc_section)
            print(f"  Adhoc spotlight generated: {adhoc_section['finding']}")
            if not adhoc_section.get("review_passed", True):
                print("  WARNING: Adhoc review scored < 8 on at least one dimension — investigate before publishing")
            if not args.dev:
                adhoc_ecb_url = _find_ecb_focus_article(
                    adhoc_theme["theme_label"], mistral_client, cost_tracker
                )
                if adhoc_ecb_url:
                    adhoc_section["ecb_article_url"] = adhoc_ecb_url
                    print(f"  ECB focus article: {adhoc_ecb_url}")
    else:
        print("  No adhoc module data for this wave.")

    print("Generating executive summary (two-pass)...")
    exec_bullets = get_exec_summary(rendered, cost_tracker, historical_context=historical_context, adhoc_section=adhoc_section, anthropic_client=anthropic_client) if rendered else []
    for item in exec_bullets:
        print(f"  [{item.get('section_id', '?')}] {item.get('bullet', '')}")

    if not args.dev and not args.adhoc_only and exec_bullets and rendered:
        print("Writing wave memory...")
        _write_wave_memory(latest_wave, exec_bullets, rendered,
                           mistral_client, tool_con, cost_tracker)

    print("Building TOC...")
    toc_html = build_toc(rendered)

    print("Building question annex...")
    annex_html = build_annex_html(con=tool_con)
    sk_annex_html = build_annex_html(con=tool_con, ui=_SK_UI)
    tool_con.close()

    print("Fetching painting thumbnail...")
    painting_inner_html = _fetch_painting_inner_html()

    print("Assembling HTML (EN)...")
    html = build_html(rendered, annex_html, exec_bullets, toc_html, painting_inner_html, latest_wave)

    retro = args.wave is not None
    out_path = OUTPUT_DIR / (f"report_q{latest_wave}.html" if retro else "report_latest.html")
    out_path.write_text(html, encoding="utf-8")
    print(f"Saved → {out_path}")

    print("Translating to Slovak...")
    sk_rendered, sk_exec_bullets = translate_to_slovak(rendered, exec_bullets, cost_tracker)
    sk_toc_html = build_toc(sk_rendered, ui=_SK_UI)
    sk_html = build_html(sk_rendered, sk_annex_html, sk_exec_bullets, sk_toc_html,
                         painting_inner_html, latest_wave, ui=_SK_UI)
    sk_path = OUTPUT_DIR / (f"report_q{latest_wave}_sk.html" if retro else "report_latest_sk.html")
    sk_path.write_text(sk_html, encoding="utf-8")
    print(f"Saved → {sk_path}")

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
    _adhoc_rendered = adhoc_section is not None
    if args.adhoc_only:
        _run_type = "adhoc"
    elif _adhoc_rendered:
        _run_type = "main+adhoc"
    else:
        _run_type = "main"
    # Derive model_mistral from whichever Mistral model was actually used
    _mistral_models = [m for m in cost_tracker["by_model"] if "mistral" in m]
    _mistral_model_used = _mistral_models[0] if _mistral_models else "mistral-small-latest"
    _run_snapshot = {
        "run_type": _run_type,
        "run_date": _now.strftime("%Y-%m-%d"),
        "run_time": _now.strftime("%H:%M:%S"),
        "wave_number": latest_wave,
        "total_cost_usd": round(cost_tracker["usd"], 5),
        "input_tokens": cost_tracker["input_tokens"],
        "output_tokens": cost_tracker["output_tokens"],
        "cache_read_tokens": cache_read_total,
        "model_sonnet": "claude-sonnet-4-6",
        "model_mistral": _mistral_model_used,
        "n_sections": len(rendered),
        "duration_seconds": round((_dt.now() - _run_start).total_seconds(), 1),
        "context_sources": {
            "annex_loaded": _annex_loaded_ok,
            "adhoc_module": _adhoc_module,
            "adhoc_topic": _adhoc_topic,
            "questionnaire_url": _questionnaire_url,
            "questionnaire_labels_parsed": _questionnaire_labels_ok,
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
