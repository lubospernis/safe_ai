"""Tests for reports/eval_harness.py — the orchestrator that composes golden
assertions + the real pipeline + the quality gate into one regression check.

Never invokes real subprocesses (run_report.py / quality_check.py are always
mocked) or real API calls — this only exercises the harness's own composition
logic against controlled inputs.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

import eval_harness

_GOLDEN_YAML = """
sections:
  - section_id: business_situation
    data_snapshot:
      SK_profit_net_balance_w37: -22.4
    assertions:
      - type: contains_number
        value: "22"
      - type: not_contains_phrase
        phrase: "recovered"
"""


@pytest.fixture
def golden_dir(tmp_path):
    d = tmp_path / "golden"
    d.mkdir()
    (d / "wave_37.yaml").write_text(_GOLDEN_YAML)
    return d


@pytest.fixture
def output_dir(tmp_path):
    d = tmp_path / "output"
    d.mkdir()
    return d


def _write_run_log(output_dir, wave, bullets):
    log = [{
        "wave_number": wave,
        "sections_summary": [
            {"section_id": "business_situation", "bullets": bullets},
        ],
    }]
    (output_dir / "run_log.json").write_text(json.dumps(log))


def _mock_proc(returncode):
    proc = MagicMock()
    proc.returncode = returncode
    return proc


def test_run_wave_missing_golden_file_returns_ok_false(golden_dir, output_dir):
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir):
        result = eval_harness.run_wave(38)  # no wave_38.yaml in golden_dir
    assert result["ok"] is False
    assert "No golden file" in result["errors"][0]


def test_run_wave_pipeline_failure_returns_ok_false(golden_dir, output_dir):
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(1)) as run_mock:
        result = eval_harness.run_wave(37)
    assert result["ok"] is False
    assert "run_report.py" in result["errors"][0]
    assert run_mock.call_count == 1  # never reaches quality_check.py


def test_run_wave_missing_run_log_entry_after_regen(golden_dir, output_dir):
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)):
        result = eval_harness.run_wave(37)  # no run_log.json written
    assert result["ok"] is False
    assert "sections_summary" in result["errors"][0]


def test_run_wave_golden_assertion_failure_surfaces_errors(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak firms' profits partly recovered — net -22pp vs prior wave net -38pp.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)):
        result = eval_harness.run_wave(37)
    assert result["ok"] is False
    assert any("recovered" in e for e in result["errors"])


def test_run_wave_all_checks_pass(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)):
        result = eval_harness.run_wave(37)
    assert result["ok"] is True
    assert result["errors"] == []


def test_run_wave_quality_gate_tier1_is_non_blocking(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    # First call = run_report.py (pass), second call = quality_check.py (tier-1, exit 3)
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", side_effect=[_mock_proc(0), _mock_proc(3)]):
        result = eval_harness.run_wave(37)
    assert result["ok"] is True


def test_run_wave_quality_gate_tier2_failure_blocks(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", side_effect=[_mock_proc(0), _mock_proc(1)]):
        result = eval_harness.run_wave(37)
    assert result["ok"] is False
    assert any("quality_check.py" in e for e in result["errors"])


def test_run_wave_missing_section_in_output_flagged(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [])
    log = json.loads((output_dir / "run_log.json").read_text())
    log[0]["sections_summary"] = [{"section_id": "other_section", "bullets": []}]
    (output_dir / "run_log.json").write_text(json.dumps(log))
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)):
        result = eval_harness.run_wave(37)
    assert result["ok"] is False
    assert any("missing from regenerated" in e for e in result["errors"])


def test_main_exits_zero_when_all_waves_pass(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.EVAL_WAVES", [37]), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("sys.argv", ["eval_harness.py"]):
        with pytest.raises(SystemExit) as exc:
            eval_harness.main()
    assert exc.value.code == 0
    assert (output_dir / "eval_harness_result.json").exists()


def test_main_exits_nonzero_when_a_wave_fails(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak firms' profits partly recovered — net -22pp.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.EVAL_WAVES", [37]), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("sys.argv", ["eval_harness.py"]):
        with pytest.raises(SystemExit) as exc:
            eval_harness.main()
    assert exc.value.code == 1


_GOLDEN_WITH_BASELINE_YAML = """
sections:
  - section_id: business_situation
    data_snapshot:
      SK_profit_net_balance_w37: -22.4
    assertions:
      - type: contains_number
        value: "22"
    judge_baseline:
      grounding: 5
      sign_correctness: 5
      magnitude_calibration: 5
      captured_date: "2026-07-21"
"""


@pytest.fixture
def golden_dir_with_baseline(tmp_path):
    d = tmp_path / "golden"
    d.mkdir()
    (d / "wave_37.yaml").write_text(_GOLDEN_WITH_BASELINE_YAML)
    return d


def test_run_wave_skips_judge_check_when_no_baseline(golden_dir, output_dir):
    """golden_dir's wave_37.yaml has no judge_baseline block — the judge should
    never be called (no API key needed for this path)."""
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("eval_harness._judge_section") as judge_mock:
        result = eval_harness.run_wave(37)
    assert result["ok"] is True
    judge_mock.assert_not_called()


def test_run_wave_judge_score_below_floor_fails(golden_dir_with_baseline, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    low_scores = {"grounding": 2, "sign_correctness": 5, "magnitude_calibration": 5, "notes": "invented number"}
    with patch("eval_harness.GOLDEN_DIR", golden_dir_with_baseline), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("eval_harness._anthropic_client", return_value=MagicMock()), \
         patch("eval_harness._judge_section", return_value=low_scores):
        result = eval_harness.run_wave(37)
    assert result["ok"] is False
    assert any("grounding=2" in e for e in result["errors"])


def test_run_wave_judge_score_within_tolerance_passes(golden_dir_with_baseline, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    close_scores = {"grounding": 4, "sign_correctness": 5, "magnitude_calibration": 5, "notes": "fine"}
    with patch("eval_harness.GOLDEN_DIR", golden_dir_with_baseline), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("eval_harness._anthropic_client", return_value=MagicMock()), \
         patch("eval_harness._judge_section", return_value=close_scores):
        result = eval_harness.run_wave(37)
    assert result["ok"] is True  # 4 >= baseline(5) - tolerance(1)


def test_run_wave_judge_parse_error_fails(golden_dir_with_baseline, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir_with_baseline), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("eval_harness._anthropic_client", return_value=MagicMock()), \
         patch("eval_harness._judge_section", return_value={"parse_error": "not json"}):
        result = eval_harness.run_wave(37)
    assert result["ok"] is False
    assert any("failed to parse" in e for e in result["errors"])


def test_approve_wave_never_writes_golden_file(golden_dir_with_baseline, output_dir, capsys):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    before = (golden_dir_with_baseline / "wave_37.yaml").read_text()
    scores = {"grounding": 5, "sign_correctness": 5, "magnitude_calibration": 5, "notes": "ok"}
    with patch("eval_harness.GOLDEN_DIR", golden_dir_with_baseline), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("eval_harness._anthropic_client", return_value=MagicMock()), \
         patch("eval_harness._judge_section", return_value=scores):
        eval_harness.approve_wave(37)
    after = (golden_dir_with_baseline / "wave_37.yaml").read_text()
    assert before == after  # untouched
    captured = capsys.readouterr()
    assert "judge_baseline" in captured.out
    assert "Paste into the golden file" in captured.out


def test_approve_requires_wave_flag(golden_dir, output_dir):
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("sys.argv", ["eval_harness.py", "--approve"]):
        with pytest.raises(SystemExit) as exc:
            eval_harness.main()
    assert exc.value.code == 1


def test_main_single_wave_via_cli_flag(golden_dir, output_dir):
    _write_run_log(output_dir, 37, [
        "Slovak profit pressures eased — net -22pp, improving from wave 36.",
    ])
    with patch("eval_harness.GOLDEN_DIR", golden_dir), \
         patch("eval_harness.OUTPUT_DIR", output_dir), \
         patch("eval_harness.EVAL_WAVES", [999]), \
         patch("eval_harness.subprocess.run", return_value=_mock_proc(0)), \
         patch("sys.argv", ["eval_harness.py", "--wave", "37"]):
        with pytest.raises(SystemExit) as exc:
            eval_harness.main()
    assert exc.value.code == 0  # only ran the explicitly-requested wave, not EVAL_WAVES
