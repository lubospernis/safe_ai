---
name: pipeline-run
description: Run the SAFE report pipeline (run_report.py, run_adhoc_report.py) or the pytest suite locally, with correct env-var loading and Python interpreter. Use when running/regenerating a report, running tests, or debugging a "MOTHERDUCK_TOKEN not set" / "command not found" / module-import error from this project.
---

# Pipeline Run

This project is MotherDuck-only — there is no local-DuckDB dev mode. Every
run (local or CI) needs `MOTHERDUCK_TOKEN`, and most runs also need
`ANTHROPIC_API_KEY` and `MISTRAL_API_KEY`. Two mistakes cause almost all
local run failures: using the wrong Python, and using `source` when the
subprocess needs the vars *exported*.

## Python interpreter

Always use the **absolute path** to the venv Python:
```bash
/Users/lubospernis/Documents/safe_ai/env/bin/python3
```
`env/bin/python3` (relative) only works if the shell's cwd is already the
project root — it silently breaks after any `cd` earlier in the session.
Default to the absolute path even when cwd looks right.

## Loading API keys

Keys live in `.env` (gitignored) at the project root.

- **Running a Python script directly in the current shell**: `source .env`
  is enough — it sets shell variables the current process's env-var reads
  (`os.environ[...]`) can see.
- **Running something that spawns a Python *subprocess*** (a wrapper script,
  a Makefile-style command, anything that shells out): plain `source` is
  **not** enough — it doesn't export the vars, so a child process won't see
  them. Use:
  ```bash
  set -a && source .env && set +a
  ```

## Running the pipeline

```bash
source .env && env/bin/python3 reports/run_report.py            # latest wave
source .env && env/bin/python3 reports/run_report.py --wave 37  # retrospective, capped at wave 37
source .env && env/bin/python3 reports/run_adhoc_report.py       # adhoc, latest wave
source .env && env/bin/python3 reports/run_adhoc_report.py --wave 37
```

`--wave N` caps data at a specific past wave — use this to regenerate an
older report or test against historical data without touching production
state. Also exposed as a `workflow_dispatch` input on
`generate_report_manual.yml` / `generate_adhoc_report_manual.yml` in CI.

**Important**: a local run only writes files under `reports/output/` — it
does **not** commit them, publish to `gh-pages`, or send a newsletter. Those
only happen via the real `generate_report.yml` / `generate_adhoc_report.yml`
GitHub Actions workflows (triggered by the dbt-completion `workflow_run`, or
manually via `gh workflow run generate_report.yml`). If a report needs to
actually go live, either push the run's output files yourself or trigger the
CI workflow — don't assume a local run alone is enough. See the
`new-adhoc-wave` skill for the full publish-diagnosis flow if a report isn't
appearing where expected.

## Running tests

```bash
/Users/lubospernis/Documents/safe_ai/env/bin/python3 -m pytest tests/ -q
```
No API keys needed — the test suite mocks all external calls (Mistral,
Anthropic, MotherDuck). If a test is hitting a real API, that's a bug in the
test, not a missing env var.

## Triggering CI workflows manually

```bash
gh workflow run generate_report.yml           # main report, publishes to gh-pages
gh workflow run generate_adhoc_report.yml      # adhoc report, publishes to gh-pages
gh workflow run generate_report_manual.yml     # past-wave retrospective, artifact only — does NOT touch production
gh run list --workflow=<file>.yml --limit 5    # check recent runs
gh run view <run-id> --log-failed              # inspect a failure
```

The `_manual.yml` variants are safe to run freely (artifact output only, no
git push, no Pages publish). The non-`_manual` workflows push commits and
publish live — treat triggering them as a real deploy action, not a test.
