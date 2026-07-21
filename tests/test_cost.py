import pytest

from cost import _Usage, _track_cost, _PRICE, check_cost_ceiling, CostCeilingExceeded, COST_CEILING_USD


def test_usage_attributes():
    u = _Usage(100, 50)
    assert u.input_tokens == 100
    assert u.output_tokens == 50
    assert u.cache_creation_input_tokens == 0
    assert u.cache_read_input_tokens == 0


def test_track_cost_basic_usd(sample_cost_tracker):
    u = _Usage(1_000_000, 0)
    _track_cost(sample_cost_tracker, "mistral-small-latest", u)
    expected = _PRICE["mistral-small-latest"]["input"]
    assert abs(sample_cost_tracker["usd"] - expected) < 0.0001


def test_track_cost_output_tokens(sample_cost_tracker):
    # 10k tokens (1/100th of a million) rather than 1M — enough to verify the
    # per-token price scaling without tripping COST_CEILING_USD for this
    # $15/M-output model (1M tokens would cost exactly $15, at the ceiling).
    u = _Usage(0, 10_000)
    _track_cost(sample_cost_tracker, "claude-sonnet-4-6", u)
    expected = _PRICE["claude-sonnet-4-6"]["output"] / 100
    assert abs(sample_cost_tracker["usd"] - expected) < 0.0001


def test_track_cost_accumulates_calls(sample_cost_tracker):
    u = _Usage(100, 50)
    _track_cost(sample_cost_tracker, "mistral-small-latest", u)
    _track_cost(sample_cost_tracker, "mistral-small-latest", u)
    assert sample_cost_tracker["calls"] == 2
    assert sample_cost_tracker["by_model"]["mistral-small-latest"]["calls"] == 2


def test_track_cost_by_model_separate(sample_cost_tracker):
    _track_cost(sample_cost_tracker, "mistral-small-latest", _Usage(100, 0))
    _track_cost(sample_cost_tracker, "claude-sonnet-4-6", _Usage(200, 0))
    assert "mistral-small-latest" in sample_cost_tracker["by_model"]
    assert "claude-sonnet-4-6" in sample_cost_tracker["by_model"]
    assert sample_cost_tracker["by_model"]["mistral-small-latest"]["input"] == 100
    assert sample_cost_tracker["by_model"]["claude-sonnet-4-6"]["input"] == 200


def test_track_cost_cache_read_discounted(sample_cost_tracker):
    u = _Usage(0, 0)
    u.cache_read_input_tokens = 1_000_000
    _track_cost(sample_cost_tracker, "claude-sonnet-4-6", u)
    # cache-read cost = input_price * 0.10
    expected = _PRICE["claude-sonnet-4-6"]["input"] * 0.10
    assert abs(sample_cost_tracker["usd"] - expected) < 0.0001


def test_track_cost_unknown_model_zero(sample_cost_tracker):
    u = _Usage(1_000_000, 1_000_000)
    _track_cost(sample_cost_tracker, "unknown-model-xyz", u)
    assert sample_cost_tracker["usd"] == 0.0


def test_track_cost_stays_under_ceiling_for_normal_run(sample_cost_tracker):
    # A realistic single call (well under a cent) must never trip the abort.
    _track_cost(sample_cost_tracker, "claude-sonnet-4-6", _Usage(2_000, 500))
    assert sample_cost_tracker["usd"] < COST_CEILING_USD


def test_track_cost_raises_when_ceiling_exceeded(sample_cost_tracker):
    # A single pathological call (e.g. a runaway retry loop feeding a huge
    # prompt) must hard-abort rather than silently keep accumulating spend.
    tokens_over_ceiling = int(COST_CEILING_USD / _PRICE["claude-opus-4-8"]["input"] * 1_000_000) + 1_000_000
    with pytest.raises(CostCeilingExceeded):
        _track_cost(sample_cost_tracker, "claude-opus-4-8", _Usage(tokens_over_ceiling, 0))
    assert sample_cost_tracker["usd"] > COST_CEILING_USD  # the triggering call is still recorded


def test_track_cost_raises_on_accumulated_total_not_just_single_call(sample_cost_tracker):
    # The ceiling is on cumulative run spend, not any single call — many small
    # calls that add up past it must also abort.
    calls_needed = int(COST_CEILING_USD / (_PRICE["claude-sonnet-4-6"]["output"] * 100_000 / 1_000_000)) + 2
    with pytest.raises(CostCeilingExceeded):
        for _ in range(calls_needed):
            _track_cost(sample_cost_tracker, "claude-sonnet-4-6", _Usage(0, 100_000))


def test_check_cost_ceiling_pre_emptive_gate_raises_on_shared_tracker():
    # Explicit call site used by run_report.py/run_adhoc_report.py before
    # starting each new parallel section — must raise directly (not only via
    # _track_cost) against an already-over-ceiling shared tracker.
    tracker = {"usd": COST_CEILING_USD + 1.0}
    with pytest.raises(CostCeilingExceeded):
        check_cost_ceiling(tracker)


def test_check_cost_ceiling_pre_emptive_gate_passes_under_ceiling():
    tracker = {"usd": 0.5}
    check_cost_ceiling(tracker)  # must not raise
