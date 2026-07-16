"""Manual LLM-as-judge eval for SAFE report sections.

Sends section bullets + data snapshot to Claude Haiku with a rubric and stores
scores in output/eval_log.json. This is Layer 3 of the eval harness — run
manually before publishing or after a major prompt change, NOT in CI.

Usage:
    source .env
    python reports/eval_judge.py --wave 37
    python reports/eval_judge.py --wave 37 --section adhoc_spotlight
"""

import argparse
import json
import os
import sys
from datetime import datetime as _dt
from pathlib import Path

import anthropic
import yaml

from cost import _anthropic_client

ROOT = Path(__file__).parent.parent
OUTPUT_DIR = ROOT / "output"
GOLDEN_DIR = ROOT / "tests" / "golden"

_RUBRIC = """
You are evaluating a section of a financial survey report. Score the bullets below on three dimensions.

DATA SNAPSHOT (ground truth from the database):
{data_snapshot}

BULLETS TO EVALUATE:
{bullets}

Score each dimension 1–5:
1. GROUNDING (1=numbers invented, 5=every cited number matches the snapshot exactly)
2. SIGN_CORRECTNESS (1=improvement/recovery language on still-negative values, 5=all sign language correct)
3. MAGNITUDE_CALIBRATION (1=intensity words clearly mismatched to pp changes, 5=all intensity words proportionate)

Respond with ONLY a JSON object, no prose:
{{"grounding": N, "sign_correctness": N, "magnitude_calibration": N, "notes": "one short sentence"}}
"""


def _load_run_log(wave: int) -> dict | None:
    """Load the most recent run log entry for the given wave."""
    log_path = OUTPUT_DIR / "run_log.json"
    if not log_path.exists():
        return None
    entries = json.loads(log_path.read_text())
    for entry in reversed(entries):
        if entry.get("wave_number") == wave:
            return entry
    return None


def _load_golden(wave: int) -> dict:
    path = GOLDEN_DIR / f"wave_{wave}.yaml"
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text())


def _judge_section(
    client: anthropic.Anthropic,
    section_id: str,
    bullets: list[str],
    data_snapshot: dict,
) -> dict:
    bullet_text = "\n".join(f"  • {b}" for b in bullets)
    snapshot_text = json.dumps(data_snapshot, indent=2)

    prompt = _RUBRIC.format(data_snapshot=snapshot_text, bullets=bullet_text)

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text.strip()
    try:
        scores = json.loads(raw)
    except json.JSONDecodeError:
        scores = {"parse_error": raw}

    scores["section_id"] = section_id
    scores["n_bullets"] = len(bullets)
    return scores


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM-as-judge eval for SAFE report")
    parser.add_argument("--wave", type=int, required=True)
    parser.add_argument("--section", type=str, default=None, help="Specific section to eval")
    args = parser.parse_args()

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ANTHROPIC_API_KEY not set. Run: source .env", file=sys.stderr)
        sys.exit(1)

    client = _anthropic_client()
    golden = _load_golden(args.wave)
    if not golden:
        print(f"No golden file for wave {args.wave} at {GOLDEN_DIR}/wave_{args.wave}.yaml")
        sys.exit(1)

    run_entry = _load_run_log(args.wave)
    if not run_entry or "sections_summary" not in run_entry:
        print(
            f"No sections_summary in run_log.json for wave {args.wave}. "
            "Re-run the report with the updated run_report.py that persists bullets."
        )
        sys.exit(1)

    sections_by_id = {s["section_id"]: s for s in run_entry["sections_summary"]}
    golden_sections = golden.get("sections", [])
    if args.section:
        golden_sections = [s for s in golden_sections if s["section_id"] == args.section]

    results = []
    for gsec in golden_sections:
        sid = gsec["section_id"]
        if sid not in sections_by_id:
            print(f"  [{sid}] not in run output — skipping")
            continue
        bullets = sections_by_id[sid].get("bullets", [])
        snapshot = gsec.get("data_snapshot", {})
        print(f"  Judging [{sid}] ({len(bullets)} bullets)...")
        scores = _judge_section(client, sid, bullets, snapshot)
        results.append(scores)
        g = scores.get("grounding", "?")
        s = scores.get("sign_correctness", "?")
        m = scores.get("magnitude_calibration", "?")
        print(f"    grounding={g} sign={s} magnitude={m}  {scores.get('notes', '')}")

    eval_entry = {
        "eval_date": _dt.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "wave": args.wave,
        "model": "claude-haiku-4-5-20251001",
        "sections": results,
    }

    log_path = OUTPUT_DIR / "eval_log.json"
    existing = json.loads(log_path.read_text()) if log_path.exists() else []
    existing.append(eval_entry)
    log_path.write_text(json.dumps(existing, indent=2, ensure_ascii=False))
    print(f"\nScores saved to {log_path}")


if __name__ == "__main__":
    main()
