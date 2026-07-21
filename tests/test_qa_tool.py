from unittest.mock import MagicMock, patch

import pytest

from qa_tool import _check_answer_grounding, ask


# ── _check_answer_grounding ──────────────────────────────────────────────────


def test_check_answer_grounding_passes_when_number_in_source():
    source_texts = ["| net_balance_wtd |\n|---|\n| 12.4 |"]
    errors = _check_answer_grounding("The net balance was 12.4%.", source_texts)
    assert errors == []


def test_check_answer_grounding_flags_fabricated_number():
    source_texts = ["| net_balance_wtd |\n|---|\n| 12.4 |"]
    errors = _check_answer_grounding("The net balance was 99.9%.", source_texts)
    assert any("99.9" in e for e in errors)


def test_check_answer_grounding_ignores_short_numbers():
    """Single-digit numbers (wave counts, list positions) are too generic to
    usefully ground — checking them would just be noise."""
    source_texts = ["Wave 38 (similarity 0.91): summary text"]
    errors = _check_answer_grounding("This was mentioned in wave 9.", source_texts)
    assert errors == []


def test_check_answer_grounding_accepts_number_from_narrative_source():
    source_texts = ["Wave 38 (similarity 0.91): Access to finance tightened to a net 22.4%."]
    errors = _check_answer_grounding("Access to finance tightened to a net 22.4%.", source_texts)
    assert errors == []


def test_check_answer_grounding_multiple_sources_combined():
    source_texts = [
        "| net_balance_wtd |\n|---|\n| 12.4 |",
        "Wave 37 (similarity 0.80): profits eased to -22.4%.",
    ]
    errors = _check_answer_grounding("Net balance 12.4%, up from -22.4% last wave.", source_texts)
    assert errors == []


# ── ask() ─────────────────────────────────────────────────────────────────────


def _text_block(text):
    b = MagicMock()
    b.text = text
    return b


def _usage():
    u = MagicMock()
    u.input_tokens = 10
    u.output_tokens = 10
    u.cache_creation_input_tokens = 0
    u.cache_read_input_tokens = 0
    return u


@pytest.fixture
def cost_tracker():
    return {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}


def test_ask_returns_grounded_answer_with_no_errors(cost_tracker):
    con = MagicMock()
    client = MagicMock()
    final_resp = MagicMock()
    final_resp.content = [_text_block("The net balance was 12.4%.")]
    final_resp.usage = _usage()
    client.messages.create.return_value = final_resp

    fake_messages = [
        {"role": "user", "content": "question"},
        {"role": "assistant", "content": []},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "| net_balance_wtd |\n|---|\n| 12.4 |"},
        ]},
    ]
    with patch("qa_tool.build_mart_catalogue", return_value="catalogue"), \
         patch("qa_tool.run_agentic_query_turns", return_value=(fake_messages, 1)):
        result = ask("What was the net balance?", con=con, client=client, cost_tracker=cost_tracker)

    assert result["answer"] == "The net balance was 12.4%."
    assert result["grounding_errors"] == []
    assert result["tool_calls"] == 1


def test_ask_flags_fabricated_number_in_answer(cost_tracker):
    con = MagicMock()
    client = MagicMock()
    final_resp = MagicMock()
    final_resp.content = [_text_block("The net balance was 55.5%.")]  # not in any tool result
    final_resp.usage = _usage()
    client.messages.create.return_value = final_resp

    fake_messages = [
        {"role": "user", "content": "question"},
        {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "| net_balance_wtd |\n|---|\n| 12.4 |"},
        ]},
    ]
    with patch("qa_tool.build_mart_catalogue", return_value="catalogue"), \
         patch("qa_tool.run_agentic_query_turns", return_value=(fake_messages, 1)):
        result = ask("What was the net balance?", con=con, client=client, cost_tracker=cost_tracker)

    assert any("55.5" in e for e in result["grounding_errors"])


def test_ask_passes_search_narrative_tool_to_run_agentic_query_turns(cost_tracker):
    con = MagicMock()
    client = MagicMock()
    final_resp = MagicMock()
    final_resp.content = [_text_block("answer")]
    final_resp.usage = _usage()
    client.messages.create.return_value = final_resp

    with patch("qa_tool.build_mart_catalogue", return_value="catalogue"), \
         patch("qa_tool.run_agentic_query_turns", return_value=([], 0)) as run_mock:
        ask("question", con=con, client=client, cost_tracker=cost_tracker)

    _, kwargs = run_mock.call_args
    assert kwargs["extra_tools"][0]["name"] == "search_narrative"
    assert kwargs["extra_tool_handler"] is not None


def test_ask_extra_tool_handler_routes_search_narrative(cost_tracker):
    con = MagicMock()
    client = MagicMock()
    final_resp = MagicMock()
    final_resp.content = [_text_block("answer")]
    final_resp.usage = _usage()
    client.messages.create.return_value = final_resp

    captured_handler = {}

    def _capture_run_agentic_query_turns(*args, **kwargs):
        captured_handler["fn"] = kwargs["extra_tool_handler"]
        return [], 0

    with patch("qa_tool.build_mart_catalogue", return_value="catalogue"), \
         patch("qa_tool.run_agentic_query_turns", side_effect=_capture_run_agentic_query_turns), \
         patch("qa_tool.retrieve_narrative", return_value=[
             {"wave_number": 38, "notable_summary": "AI adoption rose.", "similarity": 0.9},
         ]):
        ask("question", con=con, client=client, cost_tracker=cost_tracker)
        result = captured_handler["fn"]("search_narrative", {"query": "AI adoption"})

    assert "Wave 38" in result
    assert "AI adoption rose." in result


def test_ask_closes_connection_it_opened(cost_tracker):
    con = MagicMock()
    client = MagicMock()
    final_resp = MagicMock()
    final_resp.content = [_text_block("answer")]
    final_resp.usage = _usage()
    client.messages.create.return_value = final_resp

    with patch("qa_tool.build_mart_catalogue", return_value="catalogue"), \
         patch("qa_tool.run_agentic_query_turns", return_value=([], 0)), \
         patch("qa_tool._get_connection", return_value=con):
        ask("question", client=client, cost_tracker=cost_tracker)  # no con passed -> owns it

    con.close.assert_called_once()


def test_ask_does_not_close_connection_it_was_given(cost_tracker):
    con = MagicMock()
    client = MagicMock()
    final_resp = MagicMock()
    final_resp.content = [_text_block("answer")]
    final_resp.usage = _usage()
    client.messages.create.return_value = final_resp

    with patch("qa_tool.build_mart_catalogue", return_value="catalogue"), \
         patch("qa_tool.run_agentic_query_turns", return_value=([], 0)):
        ask("question", con=con, client=client, cost_tracker=cost_tracker)

    con.close.assert_not_called()


def test_main_exits_nonzero_and_withholds_answer_on_grounding_failure(capsys):
    with patch("qa_tool.ask", return_value={
        "answer": "The net balance was 55.5%.",
        "tool_calls": 1,
        "grounding_errors": ["'55.5' not found in any tool result this conversation"],
    }), \
         patch("sys.argv", ["qa_tool.py", "--question", "What was the net balance?"]):
        from qa_tool import main
        with pytest.raises(SystemExit) as exc:
            main()
    assert exc.value.code == 1
    captured = capsys.readouterr()
    assert "BLOCKED" in captured.out
    assert "55.5%" not in captured.out


def test_main_prints_answer_when_grounded(capsys):
    with patch("qa_tool.ask", return_value={
        "answer": "The net balance was 12.4%.",
        "tool_calls": 1,
        "grounding_errors": [],
    }), \
         patch("sys.argv", ["qa_tool.py", "--question", "What was the net balance?"]):
        from qa_tool import main
        main()
    captured = capsys.readouterr()
    assert "12.4%" in captured.out
