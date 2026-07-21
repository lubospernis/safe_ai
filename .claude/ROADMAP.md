# SAFE Report — Roadmap

Track ideas here. Move items to **Done** when implemented.
Claude: tick items off as they get built; don't add speculative sub-tasks.

---

## Now

- [ ] User interviews — show the report to 2–3 potential users (NBS/ECB analysts), observe where they slow down, note gaps before building more features

---

## Analytical Quality

### Grounding & Hallucination Prevention
- [x] A1: Programmatic numeric grounding check — extract numbers from bullets, verify against source DataFrame; log warnings to run_log.json (monitoring pass; not yet blocking)
- [x] A2: Exec summary number provenance check — verify exec bullet numbers appear verbatim in the section bullets they cite
- [x] A3: Wave memory number validation — block write to `ref_safe__wave_memory` if any cited number is absent from rendered bullets
- [x] A4: ECB sharpener scope guard — skip sharpener if ECB text has <2 Slovakia mentions (EA-level page)
- [x] A5: Adhoc dig-deeper SQL table whitelist + sanitized `module_id`/`qid` (SQL injection prevention)
- [x] A6: Raise main `quality_check.py` threshold from 6 → 7
- [x] A7: Persist adhoc quality scores (`adhoc_grounding`, `adhoc_coverage`, `adhoc_readability`, `adhoc_chart_alignment`, `adhoc_verdict`) to `ref_safe__run_log` in MotherDuck
- [x] A8: Promote grounding check from monitoring to blocking — **2026-07-16: fixed the remaining pp-delta false-positive class** (`_is_verified_pp_delta` in `llm.py` — a number like "13.8 pp above the EA's 32.7%" is a correctly-computed difference of two already-cited numbers, not a fabrication; confirmed against a real production false positive from a wave-37 adhoc run). With that fix plus the earlier `n=`/wave-reference/`/10`-denominator fixes, `_check_numeric_grounding` is now wired to a hard `sys.exit`-equivalent abort in both `run_report.py::_check_grounding_blocking` and `run_adhoc_report.py::_check_grounding_blocking`, called right after each report's sections are finalized. `_check_exec_provenance` (cross-section number citation) gained auto-correction (`_fix_exec_provenance_mislabels`, re-tags a bullet to the section its numbers actually belong to when it's a total mislabel) and rounding-tolerant matching, but **remains deliberately monitoring-only** — it's a design choice now, not a blocker, since the auto-fix handles the common case and a residual warning still surfaces genuinely unmatched numbers for a human to check.
- [x] A9: Gap-log triage — 73 accumulated `ref_safe__gap_log` entries de-duplicated and cross-checked against the annex; 17 rows covering structurally-unfixable content (Q12/Q13/Q14 are European-Commission-exclusive, not in this pipeline's SAFE Common-wave microdata; Bank Lending Survey and geopolitical daily-timing splits don't exist in SAFE at all) marked `status='wontfix'`. `gap_agent.py`'s `GAP_SYSTEM` prompt updated to stop flagging these three categories as fixable "Structural Gaps" going forward, so future waves don't keep re-logging fresh duplicates for the same permanent limitation.
- [x] A10: Composite financing gap (ECB Chart 4 style) — average `financing_gap_wtd` across instruments with data (bank loans, trade credit, credit lines — debt securities/equity too sparse), computed in Python (not LLM arithmetic) and always labelled as "our average across N instruments," never claimed to match ECB's unpublished exact weighting. Cited only when it adds something the per-instrument bullets don't already show (see `financing_gap`'s `focus` guidance in `config.py`).
- [x] A11: Code-enforced bullet length — `check_bullet_length` (35-word hard ceiling) added to `quality_check.py`'s deterministic gate; the ~25-word guidance existed in prompts already but was never enforced (real bullets found running 50+ words). `SECTION_CONTENT_SYSTEM`/`EXEC_SUMMARY_SYSTEM` also explicitly forbid stacking multiple sub-claims into one bullet via "while X... and Y" or an em-dash aside.
- [x] A12: Slovak translation fluency — `translate_to_slovak`'s prompt now explicitly separates "translate the MEANING, not the sentence structure" (report prose — exec bullets, section findings/bullets) from "translate faithfully/literally" (annex question text only, which must match ECB's published wording). Previously both got the same blanket "translate faithfully" instruction, which likely encouraged literal/calque translations (e.g. "Stres... sa rozšíril" for "stress widened" — not idiomatic Slovak).

---

## Next

(Roughly in priority order — see reasoning in each item.)

- [ ] Gap synthesis follow-through — `ref_safe__gap_log` was triaged 2026-07-16 (73 entries de-duplicated into ~10 real themes; 17 structurally-unfixable rows marked `wontfix` — see A9 below). Of the actionable themes, only **composite financing gap** shipped so far (A10). Still open, in priority order:
  - **Sector breakdown** (most-repeated theme, ~8 log entries) — `d3_rec` (ECB's sector recode: 1=Industry, 2=Construction, 3=Trade, 4=Services) is already staged as `sector_code` in `stg_safe__microdata.sql:45`, with real populated SK data (5 codes, 1.3k–3.8k respondents each), just never modeled past staging. Needs: add `sector_code`/`sector_label` as a dimension to `mart_safe__business_situation` and `mart_safe__expectations` (mirroring how `firm_size` is already a column there), then wire into the relevant section prompts — cite only if a sector diverges notably, not every wave.
  - **Firm-size splits on aggregate-only sections** (~6 entries: turnover by size, employment growth by size, inflation risk by size, wage expectations by size) — `firm_size` (`'all'`/`'sme'`/`'large'`) is **already a column** on the relevant marts (confirmed in Database section above); the report only ever queries `firm_size='all'`. Zero new dbt work — a second query per section plus prompt guidance to cite only when SME vs. large firms diverge substantially or the gap is growing wave-over-wave (mirrors the existing SME-divergence-note pattern already used elsewhere, ≥30pp threshold).
  - **Trade credit / credit line non-price terms coverage** — investigated 2026-07-16: `bank_loan_terms` (Q10) genuinely cannot cover this (Q10 is bank-loan-specific by ECB questionnaire design, not a coverage gap). The real target is `financing_gap` (Q5/Q9, already multi-instrument) — needs one more look at the specific gap-log entries under this theme before deciding exactly what's missing there versus already covered by the A10 composite-gap work; not yet scoped in detail.
  - Once these ship, revisit whether surfacing open (non-wontfix) `ref_safe__gap_log` rows on a status page is still worth it as a separate item, or whether triage-on-demand (as done today) is sufficient.
- [ ] Report-level feedback link — single "was this useful?" mailto/form at the foot of the report. Cheap to build, gives real signal to prioritize everything below instead of guessing.
- [ ] Make sure that ref periods are really 3m — reads as an unresolved data-integrity doubt, not a feature; cheap to audit, could be a correctness bug if wrong.
- [ ] Per-bullet exec-summary feedback — thumbs up/down + short free-text comment on each exec bullet (and, longer-term, each section bullet). **Speculative without user feedback first (see "User interviews" in Now) — hold until that's done**; this scope note (2026-07-21) is a fuller spec of the same held item, not a new one:
  - **Storage**: a new Supabase table (e.g. `report_feedback`: id, email, report_url or wave_number, section_id, bullet_text, verdict thumbs_up/down, comment, created_at), sitting alongside the existing `subscriptions`/`allowed_emails` tables in `web/supabase-setup.sql` and reusing the same per-user OTP auth + RLS pattern (`auth.jwt() ->> 'email'`) — feedback is identity-linked, not anonymous, same as today's subscription flow.
  - **Read-back into generation**: the Python pipeline can't read Supabase directly today (MotherDuck-only for pipeline data) — mirror the existing `_write_wave_memory`/wave-memory read-back pattern (`llm.py`, read at `run_report.py:205-217`): a small sync step pulls new feedback rows into a MotherDuck table each run, then folds a short summary into `historical_context` for the *next* wave's section/exec prompts, same injection point wave memory already uses.
  - **UI**: needs a place to render the widget — either directly embedded (small JS snippet) in the existing static GitHub Pages report, or in the Vercel report-hosting item below once that exists; doesn't need to wait for the Vercel item.
- [ ] Country selector — let the user pick a focal country in config (currently hard-coded to SK). Speculative without user feedback.
- [ ] Wave comparison toggle — show current wave vs previous wave side-by-side in charts. Speculative without user feedback.
- [ ] EA breakdown by country — allow drilling into EA aggregate to see member-state spread. Speculative without user feedback.
- [ ] Host reports on Vercel with interactive charts (2026-07-21) — the `web/` app (Next.js + Supabase, already live on Vercel) is a subscription portal only today: OTP-auth-gated, no MotherDuck connection, no chart library, and it only *links out* to the GitHub Pages HTML reports rather than rendering them. Hosting the reports there is a from-scratch build, not an extension of existing code. Charts today are matplotlib → PNG bytes → base64 (`reports/charts.py`), with no structured chart-data export anywhere downstream — the underlying DataFrames exist only transiently in-process.
  - **Phase 1**: export each section's chart-underlying DataFrame as JSON alongside the existing PNG (small, contained addition — `charts.py`/`run_report.py`), mirror the static report onto the Vercel app, render interactive charts client-side from that JSON. No live DB query needed; keeps the Python pipeline as the single source of truth.
  - **Phase 2** (deferred — depends on Phase 1 and overlaps the "Natural-language Q&A layer" item below): live MotherDuck queries from Vercel for real drill-down, once there's a reason beyond static charts to justify a live connection.
  - GitHub Pages publishing (`peaceiris/actions-gh-pages@v4` in `generate_report.yml`) keeps running unchanged either way — Vercel hosting is additive, not a replacement, unless later decided otherwise.
- [ ] SQL-provenance-on-hover (2026-07-21) — show the SQL query that produced a given number/stat on hover. Natural fit: every section already traces 1:1 to a literal `.sql` file via `config.py`'s `SECTIONS[i].sql_file`/`value_col`, executed at `db.py:143` (`con.execute(sql).df()`) — this is a metadata-attachment problem, not a redesign. Does **not** require the Vercel item above: the cheapest version is a tooltip/`title` attribute added directly to the existing static GitHub Pages HTML (`html_builder.py`), portable to the Vercel version once that exists. Scope for Vercel specifically if that's shipped first, otherwise ship on the static report as the faster interim path.
- [ ] Close the caching gap around the grounding safety-net (2026-07-21) — per-stage MotherDuck caching (see Done: "Add granular, hash-invalidated caching for every report pipeline stage") already means a mid-run crash does *not* force full regeneration on retry — confirmed each stage (`section_content`/`sharpen`/`classify_emphasis`/`exec_summary`/`style_en`/`translate_sk`/`style_sk`) writes to `ref_safe__pipeline_stage_cache` immediately on success, independently, so e.g. the 2026-07-20 19:31 `UngroundedNumberError` crash left EN sections/exec-summary cached and only SK translation+style needed to redo work on retry. The one real remaining gap: the grounding safety-net check is bundled *inside* `enforce_bullet_style` itself (`llm.py:2249`) — when it raises, that whole stage's cache write never happens, discarding style-fix work (Mistral calls) that already succeeded earlier in the same batch. Fix: move the grounding safety-net into its own stage (or write the style-stage cache before running the grounding check), so a grounding false positive doesn't throw away already-completed style work.

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
- [x] Adhoc-only iteration mode — superseded by `run_adhoc_report.py` becoming a fully standalone script (no `--adhoc-only` flag on `run_report.py` anymore); use `generate_adhoc_report_manual.yml` for on-demand adhoc-only runs
- [x] Exec summary model upgrades — Mistral Large 2512 for both passes of exec summary; Mistral Medium 2505 for Slovak translation; model used logged in `cost_tracker.json`
- [x] Per-run cost log — `run_log.json` (append-only array) + `ref_safe__run_log` MotherDuck table; each entry records `run_type`, `run_date`, `run_time`, wave, cost, model names
- [x] Wave memory in exec summary — `get_exec_summary()` accepts `historical_context` (last 3 waves), injected into pass 2 only with strict "only when meaningful" rule; never invents historical comparisons
- [x] Annex question texts in AI adoption sub-sections — `_fetch_question_texts()` helper pre-fetches QA/QB survey wording and injects into all 3 `_call_ai_section()` calls; `_AI_SECTION_SYSTEM` tightened to 3 bullets/600 tokens with no-invention rule
- [x] Adhoc exec summary bullet guarantee — mandatory 🔍 rule in `EXEC_SUMMARY_SYSTEM` + post-parse fallback in `get_exec_summary()` constructs one from `adhoc_section["finding"]` if Mistral omits it
- [x] Generic adhoc topic readiness — `tests/test_adhoc_generic.py`: 5 tests with electrification mock data verify `detect_adhoc_theme()` + `build_adhoc_spotlight()` produce valid HTML-compatible output for unknown module types
- [x] Human-in-the-loop newsletter gate — `send_newsletter.yml` split into `check` + `send` jobs; `send` uses `environment: newsletter-gate` (requires manual approval) when `run_type` contains "adhoc"; no-adhoc runs bypass gate automatically
- [x] Exec-summary reasoning-channel gate — `config.py` adds `exec_tier`/`subitem_tiers`/`must_lead_with` per section; `llm.py` adds code-computed `sk_ea_gap`, `historical_extremity`, `direction_reversal`, `reliable_n` signals plus a Mistral Small `classify_ecb_emphasis()` pass, threaded into `get_exec_summary()` via a `[SIGNALS]` line per section; `EXEC_SUMMARY_SYSTEM` rewritten so `policy_technical`/deprioritized topics (e.g. Q11b public support) need 2 channels to qualify instead of riding a raw wave-over-wave swing into the exec summary
- [x] Painting-thumbnail fetch retry — `_fetch_painting_inner_html()` retries transient failures (3 attempts) instead of silently omitting the block on a single network hiccup
- [x] SK newsletter — `send_newsletter.py`/`send_adhoc_newsletter.py` send each subscriber their preferred-language variant (Gmail SMTP, `lang` looked up from Supabase `allowed_emails` — see below)
- [x] Auto subscription through Vercel + Supabase — `allowed_emails` table with per-user `lang` preference, OTP-authenticated subscribe/unsubscribe flow, live-verified end to end
- [x] Two-newsletter Supabase subscriptions — `public.subscriptions` table (one row per email × newsletter_id: `safe-regular`/`safe-adhoc`) replaces the old GitHub-committed `newsletter/subscribers.json`; web app renders two independent chips/cards, each with its own subscribe/unsubscribe state; `send_newsletter.py`/`send_adhoc_newsletter.py` query Supabase directly (`reports/subscriptions_db.py`, service-role key) filtered to their respective newsletter_id
- [x] Newsletter sender switched from Resend to Gmail SMTP — Resend's trial mode blocked sending to real (non-owner) recipients; `reports/email_smtp.py` stdlib `smtplib` helper, `GMAIL_ADDRESS`/`GMAIL_16CHAR` secrets
- [x] MotherDuck-only pipeline — removed local `dev.duckdb`/`--dev` mode entirely (`db.py`, `run_report.py`, `run_adhoc_report.py`); every run (local or CI) connects to MotherDuck
- [x] Manual past-wave workflows — `generate_report_manual.yml` (main report) and `generate_adhoc_report_manual.yml` (adhoc spotlight) let you regenerate either report for a specific past wave via `workflow_dispatch`, without touching production state (no git push, no Pages publish) — output goes to a downloadable artifact
- [x] Complete Slovak translation coverage — chart PNGs (country/instrument labels, y-axis, titles), annex question text, and the web app's own UI now respect the SK/EN language choice, not just report bullets
- [x] Newsletter card enhancements — SAFE Slovakia card heading links to the latest published report; "last updated" and "next release" badges sourced from `run_report.py`'s run date and a new ECB stats-calendar scraper (`reports/fetch_release_calendar.py` → `main_safe.ref_safe__release_calendar`)
- [x] Q10 chart labeling — bank_loan_terms panel titles show "(net change in %)" and the y-axis is % formatted, scoped to Q10 only since other sections' value columns aren't all percentages
- [x] Grounding-check false-positive fixes — `_check_numeric_grounding()` no longer flags `n=` sample-size citations, wave-number references, or pressingness `/10` scale denominators (see A8 above for the calibration finding that prompted this)
- [x] Fixed DataFrame JSON-serialization crash in adhoc section cache write — `_chart_rebuild_specs`/`_response_labels` (added for the SK chart-rebuild feature) were leaking raw DataFrames into `json.dumps()`
- [x] Fixed matplotlib font inconsistency — switched from `Arial` (not installed on GitHub Actions runners, silently fell back to DejaVu Sans with a warning) to `DejaVu Sans` explicitly everywhere, so local and CI-rendered charts are visually consistent
- [x] Removed the SAFE data chatbot (`/chat`) from the Vercel app — shipped and verified working, but cut per priority change; fully isolated removal (web/lib/chat, web/app/chat, web/app/api/chat, smoke-test route, DuckDB/Mistral deps)
- [x] SK translation upgraded to `mistral-large-2512` (was `mistral-medium-2505`)
- [x] Slovak made the default language across web app, report hosting (GitHub Pages root), and both newsletters — EN remains fully available at `/en.html` / `/adhoc-en.html` and stays the pipeline's generation source-of-truth language. Fixed two latent bugs found while doing this: `send_adhoc_newsletter.py` had no per-subscriber language branching at all (always English); `send_newsletter.yml` never fetched an SK variant from `gh-pages`, so SK subscribers were silently getting English
- [x] Fixed `NBS_STYLE_GUIDE` magnitude-word thresholds to match what `evals.py`'s code-enforced gate actually checks (prompt said "notably"/"substantially" were fine at lower pp values than the gate allowed — a bullet following the prompt exactly could still fail the gate)
- [x] Fixed stale adhoc output on no-adhoc waves — `run_adhoc_report.py` now deletes leftover `report_adhoc_latest*.html` when a wave has no adhoc module, so `generate_adhoc_report.yml`'s file-existence check can't be fooled into re-publishing a previous wave's committed report under a new wave's run
- [x] Added `.claude/skills/new-adhoc-wave` and `.claude/skills/pipeline-run` project skills
- [x] Mobile-responsive report CSS — viewport meta tag, `@media (max-width: 700px)` breakpoint stacking the exec-summary+painting flexbox, full-width charts, and an `overflow-x: auto` wrapper around the 4-column annex table (previously desktop-only, no `@media` queries at all). Verified by rendering a real report through the new CSS in Safari at a narrow window width. Also fixed a real bug found in the same file: the EN/SK language-switch links were never updated after the SK-becomes-Pages-root swap and pointed at the wrong files.
- [x] Retry/backoff on every external API call — Mistral (`RetryConfig`), Anthropic (`max_retries`), MotherDuck (all connection sites), ECB HTTP fetches. Previously absent entirely; flagged across three consecutive project self-assessments as the single largest Pipeline Engineering gap.
- [x] Weekly + on-demand quality gate over live `gh-pages` content (`check_gh_pages_quality.yml`) — safety net for content that reaches `gh-pages` without going through the normal `quality_check.py` gate (e.g. a manual `git push` to the branch, which happened once this session).
- [x] Exec-summary editor given a `compute_delta` tool instead of freehand arithmetic — architectural fix eliminating the source of pp-delta/rounding grounding false-positives, rather than only detecting them after the fact via regex. Verified against the real Anthropic API (not just mocks); caught and fixed a real tool-declaration bug in the process.
- [x] Readable quarter label in report titles ("Wave 38 (Q1 2026)") — `mart_safe__slovakia_kpis.survey_period_label` was already populated, just never surfaced outside one private helper.
- [x] Grounding-check false positives on Slovak wave/months references (2026-07-21) — `_check_numeric_grounding`'s wave-reference and time-period exclusions matched English phrasing only ("wave 37", "next 12 months"), but the same check runs on Slovak-translated bullets too. Caused a real production `UngroundedNumberError` abort on the 2026-07-20 19:31 run ("v 39. vlne" — number-before-word Slovak ordinal, unlike English's word-first "wave 39"; "z vlny 38"; "12 mesiacoch"). Widened the preceding-word check to any `vln\w*` root (covers all Slovak declensions of "vlna") alongside "wave", added a following-word check for the number-first Slovak ordinal form, and extended the months exclusion to `mesiac\w*`.
