# SAFE Report — Roadmap

Track ideas here. Move items to **Done** when implemented.
Claude: tick items off as they get built; don't add speculative sub-tasks.

---

## Now

- [ ] User interviews — show the report to 2–3 potential users (NBS/ECB analysts), observe where they slow down, note gaps before building more features

---

## Next

- [ ] Include hyperlinks to the exec summary (or even hovers of numbers)
- [ ] Ask for feedback to the exec summary

- [ ] Country selector — let the user pick a focal country in the report config (currently hard-coded to SK)
- [ ] Wave comparison toggle — show current wave vs previous wave side-by-side in charts
- [ ] Scheduled auto-generation — run `run_report.py` automatically when a new SAFE wave is released and email/notify the user
- [ ] EA breakdown by country — allow drilling into EA aggregate to see member-state spread

---

## Later

- [ ] Natural-language Q&A layer — let a user ask free-text questions against the mart data (RAG or tool-use over MotherDuck)
- [ ] PDF export — generate a print-ready PDF alongside the HTML report
- [ ] Expanded country coverage in Q11 — currently SK/EA/DE; add CZ, HU, PL for Visegrád comparison
- [ ] Historical trend extension — pull waves 1–29 from `int_safe__core_questions_long` for long-run charts (pre-war, COVID, GFC comparisons)
- [ ] Alert/threshold system — flag when a KPI crosses a configurable threshold (e.g. SK bank loan gap > 10pp)

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
- [x] `mart_safe__q11_factors` dbt model — credit supply factors (Q11)
- [x] Q11 credit supply factors section in report
- [x] Removed Q26 outlook section (data too sparse at country level)
- [x] Removed loan applications section (Q7A/Q7B)
