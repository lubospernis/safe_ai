import json

from unittest.mock import MagicMock

from llm import enforce_bullet_style

# 59 words but only single-digit pp figures (grounding check always skips iv<=9) —
# a pure style violation with no grounding entanglement, matching the fixture
# used in tests/test_style_retry.py.
_LONG_BULLET = (
    "Net balance worsened from 5.0pp in wave 37 to 7.0pp in wave 38 for Slovak firms, "
    "while the EA figure moved from 3.0pp to 5.0pp over the same period, and German firms "
    "similarly saw their net balance rise from 1.5pp to 3.5pp, with all three economies "
    "showing a consistent upward trend across the two waves of collected survey data."
)
# Same content, includes a distinguishing number (42) so a revision that drops
# it can be detected as number-changing.
_LONG_BULLET_WITH_NUMBER = (
    "Net balance worsened from 5.0pp in wave 37 to 7.0pp in wave 38 for Slovak firms, "
    "a striking 42% swing that outpaced every other economy in the sample this quarter "
    "and prompted analysts to revise their outlook for the sector significantly downward "
    "heading into the next survey wave of data collection."
)


def _mistral_response(payload: dict):
    client = MagicMock()
    resp = MagicMock()
    resp.choices[0].message.content = json.dumps(payload)
    resp.usage.prompt_tokens = 20
    resp.usage.completion_tokens = 10
    client.chat.complete.return_value = resp
    return client


def _mistral_sequence(payloads: list[dict]):
    client = MagicMock()
    responses = []
    for payload in payloads:
        resp = MagicMock()
        resp.choices[0].message.content = json.dumps(payload)
        resp.usage.prompt_tokens = 20
        resp.usage.completion_tokens = 10
        responses.append(resp)
    client.chat.complete.side_effect = responses
    return client


def test_violating_bullet_gets_revised_and_passes(sample_cost_tracker):
    fixed = "Net balance worsened to 7.0pp in wave 38 from 5.0pp in wave 37 for Slovak firms."
    client = _mistral_response({"revisions": {"section:0:0": fixed}})
    rendered = [{"section_id": "sec1", "bullets": [_LONG_BULLET]}]

    new_rendered, new_exec, flagged = enforce_bullet_style(
        rendered, [], client, sample_cost_tracker, label="EN",
    )

    assert new_rendered[0]["bullets"] == [fixed]
    assert flagged == []
    client.chat.complete.assert_called_once()


def test_revision_that_drops_a_number_is_rejected(sample_cost_tracker):
    # Revision fixes length but drops the "42" — must be rejected both times,
    # leaving the original text in place and the violation flagged.
    stripped = "Net balance worsened sharply for Slovak firms this quarter, a very large swing."
    client = _mistral_sequence([
        {"revisions": {"section:0:0": stripped}},
        {"revisions": {"section:0:0": stripped}},
    ])
    rendered = [{"section_id": "sec1", "bullets": [_LONG_BULLET_WITH_NUMBER]}]

    new_rendered, new_exec, flagged = enforce_bullet_style(
        rendered, [], client, sample_cost_tracker, label="EN",
    )

    assert new_rendered[0]["bullets"] == [_LONG_BULLET_WITH_NUMBER]  # unchanged
    assert flagged  # still flagged as violating
    assert client.chat.complete.call_count == 2


def test_survivor_after_retry_is_kept_and_flagged(sample_cost_tracker):
    client = _mistral_sequence([{"revisions": {}}, {"revisions": {}}])
    rendered = [{"section_id": "sec1", "bullets": [_LONG_BULLET]}]

    new_rendered, new_exec, flagged = enforce_bullet_style(
        rendered, [], client, sample_cost_tracker, label="EN",
    )

    assert new_rendered[0]["bullets"] == [_LONG_BULLET]  # kept, not dropped/truncated
    assert any("words" in w for w in flagged)


# Grounding re-verification moved to a separate function, check_grounding_safety_net
# (2026-07-21) — see test_llm.py, where it now lives alongside _check_numeric_grounding.
# enforce_bullet_style itself no longer takes data/sections_by_id or raises
# UngroundedNumberError; a caller runs the safety-net check separately, after
# caching this function's result, so a grounding false positive can't discard
# already-completed style-fix work.
