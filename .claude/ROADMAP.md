# SAFE Report — Roadmap

Track ideas here. Move items to **Done** when implemented.
Claude: tick items off as they get built; don't add speculative sub-tasks.

---

## Now

- [ ] User interviews — show the report to 2–3 potential users (NBS/ECB analysts), observe where they slow down, note gaps before building more features

---

## Next

- [ ] SK newsletter — adapt `send_newsletter.py` to send the Slovak HTML variant (now: prerequisites met — newsletter workflow gate in place)
- [ ] Ask for feedback on the exec summary (in-report form or email link)
- [ ] Mobile viewing — report CSS is desktop-only; add responsive breakpoints
- [ ] Auto subscription through Vercel + Supabase
- [ ] Country selector — let the user pick a focal country in config (currently hard-coded to SK)
- [ ] Wave comparison toggle — show current wave vs previous wave side-by-side in charts
- [ ] EA breakdown by country — allow drilling into EA aggregate to see member-state spread
- [ ] Surface open structural gaps from `ref_safe__gap_log` on a status page or in the gap report HTML
- [ ] Add more data validation
- [ ] Make sure that ref periods are really 3m

---

## Later

- [ ] Natural-language Q&A layer — let a user ask free-text questions against the mart data (RAG or tool-use over MotherDuck)
- [ ] PDF export — generate a print-ready PDF alongside the HTML report
- [ ] Expanded country coverage in Q11 — currently SK/EA/DE; add CZ, HU, PL for Visegrád comparison
- [ ] Historical trend extension — pull waves 1–29 from `int_safe__core_questions_long` for long-run charts (pre-war, COVID, GFC comparisons)
- [ ] Alert/threshold system — flag when a KPI crosses a configurable threshold (e.g. SK bank loan gap > 10pp)
- [ ] Scheduled auto-generation — trigger `run_report.py` automatically after dlt + dbt pipeline completes successfully

---

## Done

- [x] Config-driven report orchestrator with section registry (`reports/config.py`)
- [x] Parallel Haiku interest checks + Sonnet bullet generation per section
- [x] Collapsible question annex sourced from `annex.csv`
- [x] Executive summary (Sonnet, prose, top of report)
- [x] Routed-question footnote for Q5/Q9/Q10 sections
- [x] Q10 bank terms section (always included)
- [x] Q0B pressingness section
- [x] Business situation section (Q2)
- [x] Financing need vs availability gap section (Q5/Q9)
- [x] Financing purpose section (Q6A)
- [x] `mart_safe__financing_factors` dbt model — credit supply factors (Q11)
- [x] Q11 credit supply factors section in report
- [x] Slovak report variant — `report_latest_sk.html` generated via Mistral Small translation (~$0.001 extra); language switcher pill on EN report links to SK
- [x] GitHub Pages deployment — EN report at `/report_latest.html`, SK at `/sk.html`
- [x] Newsletter integration — `send_newsletter.py` parses `#exec-summary` div for bullets
- [x] Prompt caching — system prompt is constant per run; `sign_note`/`focus` moved to user message
- [x] Cost tracking fix — `input_tokens` is already non-cached only; cache fields are additive
- [x] Quality supervisor — 4-dimension Mistral Small gate (readability, substance, coherence, sign_convention); blocks deploy if any dimension < 6
- [x] ECB question text injection — `_load_annex_question_texts()` feeds verbatim survey wording into each section prompt
- [x] Annex auto-fetch — `fetch_annex.py` downloads the ECB annex XLSX into `main_safe.ref_safe__annex` on each pipeline run; CI opens a GitHub Issue if the fetch fails
- [x] Removed Q26 outlook section (data too sparse at country level)
- [x] Removed loan applications section (Q7A/Q7B) — re-added later as `loan_applications`
- [x] Parallelised section generation — `ThreadPoolExecutor(max_workers=4)`, thread-local DuckDB connections, thread-safe cost tracking
- [x] Two-pass exec summary — cross-section analyst (Mistral, 200 tok) + editor writing JSON `[{bullet, section_id}]` (Sonnet)
- [x] "So what?" pass — Mistral Small adds implication clauses to purely-descriptive section bullets
- [x] Exec summary bullets hyperlink to source sections in HTML
- [x] Exec summary format: max 4 bullets, `**Bold label:**` + numbers encouraged
- [x] Newsletter separated into standalone `send_newsletter.yml` workflow (triggers after report generation or manual dispatch)
- [x] Run manifest — `quality_check.py` writes `quality_scores.json`; `run_report.py` writes `cost_tracker.json`; `write_run_manifest.py` logs cost, quality, git SHA, duration to `main_safe.ref_safe__run_log`
- [x] Wave memory — after each run, Mistral Small writes a 3–4 sentence notable-findings summary to `main_safe.ref_safe__wave_memory`; next run reads last 3 waves and injects as historical context into every section prompt
- [x] Gap structural log — `gap_agent.py` prompt rewritten to produce `## Structural Gaps` (upserted to `ref_safe__gap_log`, accumulating across waves) and `## Interpretation Notes` (written to `interpretation_context.md`, injected into next run's prompts)
- [x] ECB sharpener — post-generation Mistral Small pass sharpens bullets against the live ECB SAFE publication; EA comparisons and ECB framing incorporated where directly supported
- [x] Adhoc questions — `mart_safe__adhoc_responses` dbt mart + `detect_adhoc_theme()` + `build_adhoc_spotlight()` + ECB focus article search (≥90% confidence gate); collapsible Special Focus section in HTML + spotlight block in newsletter email subject & body
- [x] AI adoption spotlight — `mart_safe__ai_adoption` dbt mart + `build_ai_adoption_spotlight()` with three structured sub-sections (QA1/QA4 current use, QA2/QA3 drivers/barriers, QB1/QB2 peer expectations); agentic Claude tool-use chart generation; Pixtral chart quality check
- [x] json-repair library — all 7 Mistral JSON parse sites wrapped with `repair_json()` to handle malformed LLM output
- [x] Non-response sentinel filtering — `-9999` codes excluded from adhoc continuous charts with `response_raw BETWEEN 0 AND 100`
- [x] Modularise run_report.py — split 3,175-line monolith into 6 focused modules: `cost.py`, `db.py`, `charts.py`, `adhoc.py`, `llm.py`, `html_builder.py`; `run_report.py` reduced to thin orchestrator
- [x] Unit test framework — pytest + pytest-mock; 39 tests across `test_cost.py`, `test_charts.py`, `test_llm.py`, `test_html.py`; all passing; `_md_to_html()` extracted to `html_builder.py` fixing `**bold**` rendering in section bullets and adhoc sub-section bullets
- [x] Adhoc model upgrade — `build_ai_adoption_spotlight()` and `build_adhoc_spotlight()` upgraded from Mistral Small (220 tok) to Sonnet 4.6 (500 tok) via `anthropic_client`; Mistral is kept as fallback when no Anthropic client passed
- [x] Adhoc quality gate — `quality_check.py` extended with `extract_adhoc_text()` + a second Mistral Small supervisor call on the adhoc spotlight; fails CI if any dimension < 6
- [x] `--adhoc-only` flag — `run_report.py --adhoc-only` skips interest checks, main section generation, ECB fetch, and wave memory write; runs only adhoc spotlight + HTML assembly (cheap iteration mode)
- [x] Exec summary model upgrades — Mistral Large 2512 for both passes of exec summary; Mistral Medium 2505 for Slovak translation; model used logged in `cost_tracker.json`
- [x] Per-run cost log — `run_log.json` (append-only array) + `ref_safe__run_log` MotherDuck table; each entry records `run_type`, `run_date`, `run_time`, wave, cost, model names
- [x] Wave memory in exec summary — `get_exec_summary()` accepts `historical_context` (last 3 waves), injected into pass 2 only with strict "only when meaningful" rule; never invents historical comparisons
- [x] Annex question texts in AI adoption sub-sections — `_fetch_question_texts()` helper pre-fetches QA/QB survey wording and injects into all 3 `_call_ai_section()` calls; `_AI_SECTION_SYSTEM` tightened to 3 bullets/600 tokens with no-invention rule
- [x] Adhoc exec summary bullet guarantee — mandatory 🔍 rule in `EXEC_SUMMARY_SYSTEM` + post-parse fallback in `get_exec_summary()` constructs one from `adhoc_section["finding"]` if Mistral omits it
- [x] Generic adhoc topic readiness — `tests/test_adhoc_generic.py`: 5 tests with electrification mock data verify `detect_adhoc_theme()` + `build_adhoc_spotlight()` produce valid HTML-compatible output for unknown module types
- [x] Human-in-the-loop newsletter gate — `send_newsletter.yml` split into `check` + `send` jobs; `send` uses `environment: newsletter-gate` (requires manual approval) when `run_type` contains "adhoc"; no-adhoc runs bypass gate automatically
