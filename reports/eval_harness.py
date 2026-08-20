"""
SAFE Report Eval Harness — regression detection against golden historical waves.

Composes the previously-independent eval layers into one "regenerate wave N
end-to-end -> check it against what we already know is correct" pipeline. Each
layer already existed and was independently tested, but nothing ran them
together against real pipeline output:

  1. Golden assertions (evals.run_golden_assertions) — deterministic, catches
     confirmed historical failure modes (see tests/golden/wave_N.yaml).
  2. Blocking grounding (llm.check_grounding_safety_net) — inherited for free:
     this script shells out to the real run_report.py rather than
     reimplementing report generation, so a grounding failure aborts that
     subprocess with a non-zero exit before this script even gets to it.
  3. Tier-2 quality gate (quality_check.py --html) — the same gate that
     already blocks a live publish, run here against the regenerated
     wave-N report.
  4. LLM-judge score regression (eval_judge._judge_section) — for any golden
     section that has a judge_baseline block, compares fresh judge scores
     against that stored, human-approved floor. Sections without a
     judge_baseline are skipped here (logged, non-blocking) until one is
     captured via --approve.

Golden files (tests/golden/wave_N.yaml) are never written by this script —
--approve only ever PRINTS a ready-to-paste judge_baseline block for a human
to review and commit by hand via a normal PR. Nothing here writes to disk
except the run's own eval_harness_result.json.

Usage:
  python reports/eval_harness.py                # run all EVAL_WAVES
  python reports/eval_harness.py --wave 37       # run one wave only
  python reports/eval_harness.py --wave 37 --no-cache   # force full regeneration
  python reports/eval_harness.py --wave 37 --approve    # print current judge
                                                          # scores as a diff
                                                          # against the stored
                                                          # judge_baseline, for
                                                          # manual review

Required environment variables (same as run_report.py / quality_check.py):
  MOTHERDUCK_TOKEN, ANTHROPIC_API_KEY, MISTRAL_API_KEY
"""

import argparse
import json
import subprocess
import sys
from datetime import date
from pathlib import Path

import yaml

from cost import _anthropic_client
from eval_judge import _judge_section
from evals import run_golden_assertions

ROOT = Path(__file__).parent.parent
REPORTS_DIR = Path(__file__).parent
OUTPUT_DIR = REPORTS_DIR / "output"
GOLDEN_DIR = ROOT / "tests" / "golden"

# Fixed, deliberately small set of historical waves — not "every wave," to keep
# the weekly CI run's LLM cost bounded. Extend by adding a new tests/golden/
# wave_N.yaml and appending N here.
EVAL_WAVES = [37]

# A judge score below (baseline - _JUDGE_TOLERANCE), or below this absolute
# floor regardless of baseline, fails the harness. 1-point tolerance absorbs
# the LLM judge's normal run-to-run variance without masking a real drop.
_JUDGE_TOLERANCE = 1
_JUDGE_FLOOR = 3
_JUDGE_DIMENSIONS = ("grounding", "sign_correctness", "magnitude_calibration")


def _load_run_log_entry(wave: int) -> dict | None:
    """Most recent run_log.json entry for the given wave (mirrors
    eval_judge.py's _load_run_log, kept local to avoid a Layer-3 import for a
    plain data lookup)."""
    log_path = OUTPUT_DIR / "run_log.json"
    if not log_path.exists():
        return None
    entries = json.loads(log_path.read_text())
    for entry in reversed(entries):
        if entry.get("wave_number") == wave:
            return entry
    return None


def _load_golden_sections(wave: int) -> list[dict]:
    path = GOLDEN_DIR / f"wave_{wave}.yaml"
    if not path.exists():
        return []
    golden = yaml.safe_load(path.read_text())
    return golden.get("sections", [])


def _regenerate(wave: int, no_cache: bool) -> tuple[dict | None, str | None]:
    """Run run_report.py --wave N and return (sections_by_id, None) on success,
    or (None, error_message) on failure. Shared by run_wave and approve_wave so
    there's exactly one way this repo regenerates a historical wave."""
    print(f"[wave {wave}] Regenerating report (run_report.py --wave {wave})...")
    cmd = [sys.executable, str(REPORTS_DIR / "run_report.py"), "--wave", str(wave)]
    if no_cache:
        cmd.append("--no-cache")
    proc = subprocess.run(cmd, cwd=REPORTS_DIR)
    if proc.returncode != 0:
        return None, (
            f"run_report.py --wave {wave} exited {proc.returncode} — likely a "
            "blocking grounding failure (UngroundedNumberError) or other pipeline "
            "error; see the log above for the real cause"
        )

    run_entry = _load_run_log_entry(wave)
    if not run_entry or "sections_summary" not in run_entry:
        return None, f"No sections_summary in run_log.json for wave {wave} after regeneration"

    return {s["section_id"]: s for s in run_entry["sections_summary"]}, None


def _check_judge_scores(wave: int, gsec: dict, bullets: list[str]) -> list[str]:
    """Compare fresh judge scores against gsec's stored judge_baseline. Returns
    an empty list (and makes no API call) if the section has no baseline yet —
    this check only activates once a human has captured one via --approve."""
    baseline = gsec.get("judge_baseline")
    if not baseline:
        return []
    sid = gsec["section_id"]
    scores = _judge_section(_anthropic_client(), sid, bullets, gsec.get("data_snapshot", {}))
    if "parse_error" in scores:
        return [f"[{sid}] judge response failed to parse: {scores['parse_error']}"]
    errors = []
    for dim in _JUDGE_DIMENSIONS:
        if dim not in baseline:
            continue
        floor = max(baseline[dim] - _JUDGE_TOLERANCE, _JUDGE_FLOOR)
        actual = scores.get(dim)
        if actual is None or actual < floor:
            errors.append(
                f"[{sid}] judge {dim}={actual} dropped below floor {floor} "
                f"(baseline {baseline[dim]}, tolerance {_JUDGE_TOLERANCE}): {scores.get('notes', '')}"
            )
    return errors


def run_wave(wave: int, no_cache: bool = False) -> dict:
    """Regenerate wave N end-to-end and check it against its golden file.

    Never raises for a check failure — only for the harness's own inability to
    proceed (e.g. missing golden file) does it short-circuit early; otherwise
    it always returns a result dict and lets the caller aggregate/exit.
    """
    print(f"\n{'=' * 70}\nEval harness — wave {wave}\n{'=' * 70}")

    golden_sections = _load_golden_sections(wave)
    if not golden_sections:
        msg = f"No golden file for wave {wave} at {GOLDEN_DIR}/wave_{wave}.yaml"
        print(f"  {msg}")
        return {"wave": wave, "ok": False, "errors": [msg]}

    sections_by_id, err = _regenerate(wave, no_cache)
    if err:
        return {"wave": wave, "ok": False, "errors": [err]}

    errors: list[str] = []
    for gsec in golden_sections:
        sid = gsec["section_id"]
        if sid not in sections_by_id:
            errors.append(f"[{sid}] section missing from regenerated wave {wave} output")
            continue
        bullets = sections_by_id[sid].get("bullets", [])
        errors.extend(run_golden_assertions(sid, bullets, wave))
        if gsec.get("judge_baseline"):
            errors.extend(_check_judge_scores(wave, gsec, bullets))
        else:
            print(f"[wave {wave}] [{sid}] no judge_baseline captured yet — "
                  "skipping judge-score regression (run --approve to capture one)")

    print(f"[wave {wave}] Golden assertions + judge regression: "
          f"{'PASS' if not errors else f'{len(errors)} FAILURE(S)'}")
    for e in errors:
        print(f"  - {e}")

    report_html = OUTPUT_DIR / f"report_q{wave}.html"
    print(f"[wave {wave}] Quality gate (quality_check.py --html {report_html.name})...")
    qc = subprocess.run(
        [sys.executable, str(REPORTS_DIR / "quality_check.py"), "--html", str(report_html)],
        cwd=REPORTS_DIR,
    )
    if qc.returncode == 3:
        print(f"[wave {wave}] Quality gate: tier-1 style issues only (non-blocking)")
    elif qc.returncode != 0:
        errors.append(
            f"quality_check.py --html {report_html.name} failed (exit {qc.returncode}) "
            "— tier-2 content-quality failure, see quality_scores*.json"
        )

    return {"wave": wave, "ok": not errors, "errors": errors}


def approve_wave(wave: int, no_cache: bool = False) -> None:
    """Regenerate wave N, judge each golden section, and print a ready-to-paste
    judge_baseline block plus a diff against the currently-stored baseline (if
    any). Never writes to disk — a human reviews this output and pastes the
    accepted block into tests/golden/wave_N.yaml by hand, then commits it via
    a normal PR. This is the only sanctioned way a judge_baseline gets set."""
    golden_sections = _load_golden_sections(wave)
    if not golden_sections:
        print(f"No golden file for wave {wave} at {GOLDEN_DIR}/wave_{wave}.yaml")
        sys.exit(1)

    sections_by_id, err = _regenerate(wave, no_cache)
    if err:
        print(f"ERROR: {err}")
        sys.exit(1)

    client = _anthropic_client()
    print(f"\n{'=' * 70}\nProposed judge_baseline for wave {wave} — review before pasting\n{'=' * 70}")
    for gsec in golden_sections:
        sid = gsec["section_id"]
        if sid not in sections_by_id:
            print(f"[{sid}] not in regenerated output — skipping")
            continue
        bullets = sections_by_id[sid].get("bullets", [])
        scores = _judge_section(client, sid, bullets, gsec.get("data_snapshot", {}))
        old = gsec.get("judge_baseline", {})
        print(f"\n  section_id: {sid}")
        for dim in _JUDGE_DIMENSIONS:
            new_val = scores.get(dim, "?")
            old_val = old.get(dim, "(none)")
            marker = "  <- changed" if dim in old and old[dim] != new_val else ""
            print(f"    {dim}: {old_val} -> {new_val}{marker}")
        print(f"    notes: {scores.get('notes', '')}")
        print("    Paste into the golden file if you accept these scores:")
        print("    judge_baseline:")
        for dim in _JUDGE_DIMENSIONS:
            if dim in scores:
                print(f"      {dim}: {scores[dim]}")
        print(f"      captured_date: \"{date.today().isoformat()}\"")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="SAFE report eval harness — regression check against golden historical waves"
    )
    parser.add_argument("--wave", type=int, default=None,
                        help="Run a single wave only (default: all EVAL_WAVES)")
    parser.add_argument("--no-cache", action="store_true",
                        help="Force full regeneration, bypassing the pipeline-stage cache")
    parser.add_argument("--approve", action="store_true",
                        help="Print a proposed judge_baseline for review instead of "
                             "running the pass/fail harness (requires --wave; never writes)")
    args = parser.parse_args()

    if args.approve:
        if not args.wave:
            print("--approve requires --wave N")
            sys.exit(1)
        approve_wave(args.wave, no_cache=args.no_cache)
        return

    waves = [args.wave] if args.wave else EVAL_WAVES
    results = [run_wave(w, no_cache=args.no_cache) for w in waves]

    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / "eval_harness_result.json").write_text(json.dumps(results, indent=2))

    print(f"\n{'=' * 70}\nEval harness summary\n{'=' * 70}")
    all_ok = True
    for r in results:
        status = "PASS" if r["ok"] else "FAIL"
        print(f"  wave {r['wave']}: {status}"
              + (f" ({len(r['errors'])} issue(s))" if r["errors"] else ""))
        if not r["ok"]:
            all_ok = False
            for e in r["errors"]:
                print(f"::error::[wave {r['wave']}] {e}")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
