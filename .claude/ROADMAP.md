# SAFE Report — Roadmap

Track ideas here. Move items to **Done** when implemented.
Claude: tick items off as they get built; don't add speculative sub-tasks.

---

## Now

- [ ] User interviews — show the report to 2–3 potential users (NBS/ECB analysts), observe where they slow down, note gaps before building more features

---

## Next

- [ ] Include hyperlinks from exec summary bullets to the relevant section
- [ ] SK translation quality — Mistral Small output is fluent but not analyst-grade; consider prompt refinement or few-shot examples
- [ ] Adhoc questions — surface per-wave ad-hoc module questions as a collapsible special section
- [ ] Gap report — improve clarity and coverage of the gap analysis output
- [ ] Ask for feedback on the exec summary (in-report form or email link)
- [ ] SK newsletter — adapt `send_newsletter.py` to send the Slovak HTML variant
- [ ] Mobile viewing — report CSS is desktop-only; add responsive breakpoints
- [ ] Auto subscription through Vercel + Supabase
- [ ] Adhoc questions queries fully automatic (wave-adaptive SQL)?
- [ ] Country selector — let the user pick a focal country in config (currently hard-coded to SK)
- [ ] Wave comparison toggle — show current wave vs previous wave side-by-side in charts
- [ ] EA breakdown by country — allow drilling into EA aggregate to see member-state spread

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
