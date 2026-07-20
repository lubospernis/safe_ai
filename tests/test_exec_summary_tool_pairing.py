from unittest.mock import MagicMock

from llm import get_exec_summary


def _usage():
    u = MagicMock()
    u.input_tokens = 10
    u.output_tokens = 10
    u.cache_creation_input_tokens = 0
    u.cache_read_input_tokens = 0
    return u


def _text_response(text: str):
    resp = MagicMock()
    resp.content = [MagicMock(type="text", text=text)]
    resp.usage = _usage()
    return resp


def _truncated_tool_use_response():
    """Simulates the model getting cut off by max_tokens mid tool-call: content
    still has a tool_use block, but stop_reason is 'max_tokens', not 'tool_use'."""
    block = MagicMock()
    block.type = "tool_use"
    block.id = "toolu_truncated"
    block.input = {"a": 61.23, "b": 51.33, "label": "SK vs DE gap"}
    resp = MagicMock()
    resp.content = [block]
    resp.stop_reason = "max_tokens"
    resp.usage = _usage()
    return resp


def _emit_bullets_response():
    block = MagicMock()
    block.type = "tool_use"
    block.name = "emit_exec_bullets"
    block.id = "toolu_emit"
    block.input = {"bullets": [{"bullet": "Slovak inflation risk outpaces the EA.",
                                 "section_id": "expectations_risk"}]}
    resp = MagicMock()
    resp.content = [block]
    resp.usage = _usage()
    return resp


def test_truncated_tool_call_does_not_leave_dangling_tool_use(sample_cost_tracker):
    """A response with stop_reason='max_tokens' that still contains a tool_use
    block must get a paired tool_result before the conversation continues —
    otherwise the next API call 400s on 'tool_use ids were found without
    tool_result blocks'. This reproduces a live production failure. The loop
    must then keep going (retry, within the turn budget) rather than jumping
    straight to emit_exec_bullets — proven here by the mock needing a 4th
    response (a normal no-tool-use turn) before the final emit call."""
    client = MagicMock()
    client.messages.create.side_effect = [
        _text_response("Cross-cutting theme: inflation risk divergence."),  # pass 1
        _truncated_tool_use_response(),                                    # loop turn 1: truncated
        _text_response("Ready to finalize."),                              # loop turn 2: no more tools
        _emit_bullets_response(),                                          # final emit
    ]

    rendered_sections = [{
        "section_id": "expectations_risk",
        "title": "Inflation risk",
        "finding": "Slovak upside inflation risk rose",
        "sign_note": "positive = more firms see upside risk",
        "bullets": ["A net 61% of Slovak firms saw upside risk (n=242)."],
    }]

    result = get_exec_summary(
        rendered_sections, sample_cost_tracker, anthropic_client=client,
    )

    assert client.messages.create.call_count == 4

    # The tool_result for the truncated tool_use must have been appended
    # immediately after its assistant message, with no user-text message in between.
    final_call_messages = client.messages.create.call_args_list[-1].kwargs["messages"]
    tool_use_msg_idx = next(
        i for i, m in enumerate(final_call_messages)
        if m["role"] == "assistant"
        and any(getattr(b, "type", None) == "tool_use" for b in m["content"])
    )
    paired = final_call_messages[tool_use_msg_idx + 1]
    assert paired["role"] == "user"
    assert any(item.get("type") == "tool_result" and item.get("tool_use_id") == "toolu_truncated"
               for item in paired["content"])

    assert result == [{"bullet": "Slovak inflation risk outpaces the EA.",
                        "section_id": "expectations_risk"}]
