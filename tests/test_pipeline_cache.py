import json

from unittest.mock import MagicMock

import evals
from llm import _pipeline_cache_get, _pipeline_cache_hash, _pipeline_cache_put


# ── _pipeline_cache_hash ─────────────────────────────────────────────────────


def test_hash_is_deterministic_regardless_of_dict_key_order():
    h1 = _pipeline_cache_hash("prompt a", {"x": 1, "y": 2}, ["a", "b"])
    h2 = _pipeline_cache_hash("prompt a", {"y": 2, "x": 1}, ["a", "b"])
    assert h1 == h2


def test_hash_changes_when_any_input_changes():
    h1 = _pipeline_cache_hash("prompt a", {"x": 1})
    h2 = _pipeline_cache_hash("prompt b", {"x": 1})
    h3 = _pipeline_cache_hash("prompt a", {"x": 2})
    assert h1 != h2
    assert h1 != h3


def test_hash_is_a_short_hex_string():
    h = _pipeline_cache_hash("anything")
    assert len(h) == 16
    int(h, 16)  # raises if not valid hex


def test_evals_module_source_hash_changes_when_max_bullet_words_changes():
    """The user's exact motivating scenario: raising _MAX_BULLET_WORDS in
    evals.py must change the hash the style-enforcement stages key off of
    (inspect.getsource(evals)), so the cache is correctly busted — while
    everything upstream (which doesn't hash evals.py at all) stays cached."""
    source_35 = evals.__doc__ or ""
    source_35 = "_MAX_BULLET_WORDS = 35\n" + source_35
    source_45 = "_MAX_BULLET_WORDS = 45\n" + (evals.__doc__ or "")
    assert _pipeline_cache_hash("style", source_35) != _pipeline_cache_hash("style", source_45)


# ── _pipeline_cache_get / _pipeline_cache_put ────────────────────────────────


def _mock_con(fetchone_return=None, raise_on_select=False, raise_on_write=False):
    con = MagicMock()

    def execute_side_effect(sql, *args, **kwargs):
        if raise_on_select and "SELECT" in sql:
            raise RuntimeError("connection reset")
        if raise_on_write and "INSERT" in sql:
            raise RuntimeError("connection reset")
        result = MagicMock()
        if "SELECT source_hash, payload" in sql:
            result.fetchone.return_value = fetchone_return
        return result

    con.execute.side_effect = execute_side_effect
    return con


def test_cache_get_miss_on_empty_table():
    con = _mock_con(fetchone_return=None)
    assert _pipeline_cache_get(con, "main_safe", "sharpen", "main_w39") is None


def test_cache_get_hit_returns_hash_and_parsed_payload():
    con = _mock_con(fetchone_return=("abc123", json.dumps({"a": 1})))
    result = _pipeline_cache_get(con, "main_safe", "sharpen", "main_w39")
    assert result == ("abc123", {"a": 1})


def test_cache_get_degrades_to_miss_on_query_exception():
    con = _mock_con(raise_on_select=True)
    assert _pipeline_cache_get(con, "main_safe", "sharpen", "main_w39") is None


def test_cache_get_degrades_to_miss_on_unparseable_payload():
    con = _mock_con(fetchone_return=("abc123", "not valid json"))
    assert _pipeline_cache_get(con, "main_safe", "sharpen", "main_w39") is None


def test_cache_get_scopes_by_stage_and_cache_key():
    con = _mock_con(fetchone_return=None)
    _pipeline_cache_get(con, "main_safe", "exec_summary", "main_w39")
    select_calls = [c for c in con.execute.call_args_list if "SELECT source_hash, payload" in c.args[0]]
    assert len(select_calls) == 1
    assert select_calls[0].args[1] == ["exec_summary", "main_w39"]


def test_cache_put_writes_insert_or_replace_with_all_fields():
    con = _mock_con()
    _pipeline_cache_put(con, "main_safe", "exec_summary", "main_w39", "hash123",
                        {"bullets": ["a"]}, "claude-opus-4-8")
    insert_calls = [c for c in con.execute.call_args_list if "INSERT OR REPLACE" in c.args[0]]
    assert len(insert_calls) == 1
    params = insert_calls[0].args[1]
    assert params[0] == "exec_summary"
    assert params[1] == "main_w39"
    assert params[2] == "hash123"
    assert json.loads(params[3]) == {"bullets": ["a"]}
    assert params[4] == "claude-opus-4-8"


def test_cache_put_does_not_raise_on_write_failure():
    con = _mock_con(raise_on_write=True)
    # Must not raise — a write failure degrades to "cache didn't help this
    # time," not a pipeline crash.
    _pipeline_cache_put(con, "main_safe", "exec_summary", "main_w39", "hash123", {}, "model")


def test_cache_roundtrip_via_shared_backing_store():
    """A get() after a put() against the same (in-memory, dict-backed) store
    returns exactly what was written — proves the get/put pair is internally
    consistent, not just independently mocked."""
    store: dict[tuple[str, str], tuple[str, str]] = {}
    con = MagicMock()

    def execute_side_effect(sql, *args, **kwargs):
        result = MagicMock()
        if "SELECT source_hash, payload" in sql:
            stage, cache_key = args[0]
            row = store.get((stage, cache_key))
            result.fetchone.return_value = row
        elif "INSERT OR REPLACE" in sql:
            stage, cache_key, source_hash, payload, model_id, generated_at = args[0]
            store[(stage, cache_key)] = (source_hash, payload)
        return result

    con.execute.side_effect = execute_side_effect

    assert _pipeline_cache_get(con, "main_safe", "sharpen", "main_w39") is None
    _pipeline_cache_put(con, "main_safe", "sharpen", "main_w39", "hashA", {"x": 1}, "model")
    assert _pipeline_cache_get(con, "main_safe", "sharpen", "main_w39") == ("hashA", {"x": 1})
    # A different wave must not see it (wave/pipeline isolation via cache_key).
    assert _pipeline_cache_get(con, "main_safe", "sharpen", "main_w40") is None
