---
name: new-adhoc-wave
description: Onboard a new ECB SAFE adhoc survey wave (a previously unseen QA/QB module) into the pipeline, and diagnose why an adhoc report isn't publishing. Use when a new wave lands with an unrecognized adhoc module, when generate_adhoc_report.yml fails or never seems to have run, or when the adhoc report/card link is missing or 404s.
---

# New Adhoc Wave

Converts the checklist in `.claude/CLAUDE.md` ("New Adhoc Wave Checklist") into
a runnable procedure, plus troubleshooting for the failure modes actually
observed in this pipeline. No code changes are required for a new module —
the pipeline is generic — but multiple things must independently succeed for
a report to actually reach subscribers, and a break in any one of them is
silent unless checked.

## Quick diagnosis: "the adhoc report isn't showing up"

Work top-down; stop at the first failure you find.

1. **Does gh-pages actually have adhoc files?**
   ```bash
   git fetch origin gh-pages
   git ls-tree -r --name-only origin/gh-pages | grep -i adhoc
   ```
   Expect `adhoc.html` (SK, default) and `adhoc-en.html` (EN). If empty, the
   publish step never ran successfully — go to step 2.

2. **Has `generate_adhoc_report.yml` ever run, and did it pass?**
   ```bash
   gh run list --workflow=generate_adhoc_report.yml --limit 5
   ```
   Empty output = never triggered (it only fires on `workflow_run` from the
   dbt workflow, or manual `workflow_dispatch` — it does NOT run on a schedule).
   A `failure` row = it ran and was blocked; inspect logs:
   ```bash
   gh run view <run-id> --log-failed
   ```

3. **Is the failure the quality gate (`quality_check.py`), not infra?**
   Look for `DETERMINISTIC CHECK FAIL` or a quality-dimension score below
   threshold in the failed log. This is `reports/evals.py`'s code-enforced
   checks (sign language, magnitude calibration, bare response codes) plus
   an LLM supervisor — it is doing its job correctly if it blocks. Do not
   bypass it; fix the underlying bullet generation. A recurring root cause
   found once already: `NBS_STYLE_GUIDE` in `reports/llm.py` telling the LLM
   different magnitude-word thresholds than `evals.py` actually enforces —
   check these are still in sync if this class of failure recurs.

4. **Is `reports/output/latest_adhoc_links.json` (and the HTML it points to)
   actually committed?** A local/manual pipeline run produces files that sit
   uncommitted unless explicitly `git add`ed — the web app's adhoc card reads
   this file straight from `raw.githubusercontent.com/main`, so an
   uncommitted-but-locally-generated run is invisible to users even if the
   report itself looks fine on disk.
   ```bash
   git status --porcelain reports/output/
   ```

5. **Once gh-pages has the files**: confirm the actual URLs resolve —
   ```bash
   curl -s -o /dev/null -w "%{http_code}\n" "https://lubospernis.github.io/safe_ai/adhoc.html"
   ```
   200 = done. If the web app's card still doesn't show a link, check
   `reports/output/latest_adhoc_links.json`'s content matches what's live
   (see step 4) — the app has no other data source for this link.

## Onboarding a new adhoc module (checklist)

No code changes required — the pipeline handles unknown QA/QB modules
generically. In order:

1. **dbt mart update**: `dbt run --select mart_safe__adhoc_responses` after
   the new wave lands in the microdata. This is the single source of truth
   for all adhoc modules.
2. **Annex entries**: run `fetch_annex.py` (or let CI do it) so the new
   module's question texts land in `ref_safe__annex`. `_fetch_question_texts()`
   in `reports/adhoc.py` queries this table live.
3. **Verify detection**: run `run_adhoc_report.py` and confirm
   `detect_adhoc_theme()` logs the correct module ID. The theme label comes
   from `_MODULE_THEME_FALLBACK` in `reports/adhoc.py` if known, otherwise
   from a Mistral Small classification pass.
4. **Optional fallback label**: if the auto-classified label is poor, add the
   module ID to `_MODULE_THEME_FALLBACK` in `reports/adhoc.py`.
5. **Questionnaire URL**: `reports/questionnaire.py` derives the PDF URL from
   the dlt load timestamp automatically. If the run log shows
   `questionnaire_labels_parsed: false`, add the wave's entry to
   `_PERIOD_TO_QUESTIONNAIRE_SUFFIX` in `questionnaire.py` as a confirmed
   override (`{YYYY}{MM}` of the ECB release month).
6. **Quality gate**: after a full run, `quality_check.py` runs a second
   supervisor call on the adhoc spotlight (`ADHOC_PASS_THRESHOLD = 8`,
   stricter than the main report's 7). A failure here blocks CI — see
   "Quick diagnosis" step 3 above.
7. **Actually publish**: a local/manual run does not publish to gh-pages or
   commit the links file. Only `generate_adhoc_report.yml` (via its
   `workflow_run` trigger after dbt, or manual `workflow_dispatch`) does
   both. Trigger it explicitly if you need the wave live now:
   ```bash
   gh workflow run generate_adhoc_report.yml
   ```
8. **Newsletter**: `send_adhoc_newsletter.yml` triggers automatically after
   a successful `generate_adhoc_report.yml` run, gated behind the
   `newsletter-gate` environment (requires manual approval — GitHub emails
   the reviewer). It sends per-subscriber in their preferred language
   (`allowed_emails.lang`, default `sk`) via `reports/send_adhoc_newsletter.py`.

When a task here corresponds to a `.claude/ROADMAP.md` item, tick it off
there per this project's standing convention — don't duplicate the tracking
in this file.
