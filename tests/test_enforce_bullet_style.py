import json

import pandas as pd
import pytest
from unittest.mock import MagicMock

from llm import UngroundedNumberError, enforce_bullet_style

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


def test_grounding_recheck_catches_newly_ungrounded_bullet(sample_cost_tracker, net_balance_df):
    """A bullet that's clean style-wise but cites a number never present in the
    source data (as if introduced by ECB-sharpening or SK translation after the
    original post-generation grounding check already passed) must still be
    caught — this is a correctness bug, not a style nit, so it raises."""
    client = MagicMock()  # style check passes, so chat.complete should never be called
    rendered = [{
        "section_id": "sec1",
        "bullets": ["The net balance shifted to a striking 87% this wave for Slovak firms."],
    }]
    data = {"sec1": net_balance_df}
    sections_by_id = {"sec1": {"value_col": "net_balance_wtd"}}

    with pytest.raises(UngroundedNumberError):
        enforce_bullet_style(
            rendered, [], client, sample_cost_tracker, label="EN",
            data=data, sections_by_id=sections_by_id,
        )

    client.chat.complete.assert_not_called()


def test_grounding_recheck_passes_clean_bullets(sample_cost_tracker, net_balance_df):
    client = MagicMock()
    rendered = [{
        "section_id": "sec1",
        "bullets": ["Net balance rose to 7.0pp in wave 38 for Slovak firms."],
    }]
    data = {"sec1": net_balance_df}
    sections_by_id = {"sec1": {"value_col": "net_balance_wtd"}}

    new_rendered, new_exec, flagged = enforce_bullet_style(
        rendered, [], client, sample_cost_tracker, label="EN",
        data=data, sections_by_id=sections_by_id,
    )

    assert flagged == []
    client.chat.complete.assert_not_called()


def test_grounding_recheck_handles_slovak_decimal_comma(sample_cost_tracker):
    """Reproduces a real production failure: Slovak formats decimals with a
    comma ('43,8' means 43.8), and the translation pass renders numbers this
    way despite being told to keep them unchanged. The grounding safety-net
    was tokenizing '43,8' as two separate numbers ('43' and '8'), flagging a
    real, correctly cited value as fabricated and aborting every SK run."""
    df = pd.DataFrame([{
        "wave_number": 39, "country_code": "SK", "net_balance_wtd": 43.8,
        "n_respondents": 76, "firm_size": "all",
    }])
    client = MagicMock()
    rendered = [{
        "section_id": "sec1",
        "bullets": ["Čistých 43,8 % slovenských firiem (n=76) uviedlo sprísnenie podmienok."],
    }]
    data = {"sec1": df}
    sections_by_id = {"sec1": {"value_col": "net_balance_wtd"}}

    new_rendered, new_exec, flagged = enforce_bullet_style(
        rendered, [], client, sample_cost_tracker, label="SK",
        data=data, sections_by_id=sections_by_id,
    )

    assert flagged == []
    client.chat.complete.assert_not_called()
