from cost import _Usage, _track_cost, _PRICE


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
    u = _Usage(0, 1_000_000)
    _track_cost(sample_cost_tracker, "claude-sonnet-4-6", u)
    expected = _PRICE["claude-sonnet-4-6"]["output"]
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
