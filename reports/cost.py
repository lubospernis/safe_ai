"""Cost tracking and LLM client initialisation."""

import os

import anthropic
from mistralai import Mistral
from mistralai.utils import BackoffStrategy, RetryConfig

_PRICE = {
    "claude-sonnet-4-6":         {"input": 3.00,  "output": 15.00},
    "claude-opus-4-8":           {"input": 15.00, "output": 75.00},
    "claude-haiku-4-5-20251001": {"input": 1.00,  "output": 5.00},
    "mistral-small-latest":      {"input": 0.10,  "output": 0.30},
    "mistral-medium-latest":     {"input": 0.40,  "output": 2.00},
    "mistral-medium-2505":       {"input": 0.40,  "output": 2.00},
    "mistral-large-2512":        {"input": 2.00,  "output": 6.00},
    "pixtral-12b-2409":          {"input": 0.15,  "output": 0.15},
}

# 2026-07-21: consolidated from run_report.py/run_adhoc_report.py, which each
# independently defined an identical COST_CEILING_USD/CostCeilingExceeded/
# _check_cost_ceiling() (2026-07-07). Same env var name and $15.00 default as
# the mechanism this replaces, so no external config changes.
COST_CEILING_USD = float(os.environ.get("COST_CEILING_USD", "15.0"))


class CostCeilingExceeded(RuntimeError):
    """Raised when a run's cumulative spend crosses COST_CEILING_USD — a hard
    abort, not a warning, since nothing downstream of a runaway cost is worth
    spending further API budget to finish."""
    pass


def check_cost_ceiling(tracker: dict) -> None:
    """Raise CostCeilingExceeded if tracker["usd"] has crossed COST_CEILING_USD.

    Called automatically by _track_cost after every tracked call, so it's a
    real guard everywhere cost is tracked (llm.py, adhoc.py, gap_agent.py, not
    just wherever someone remembered to add a checkpoint). ALSO called
    explicitly by run_report.py/run_adhoc_report.py as a pre-emptive gate
    before starting each new section in the parallel (ThreadPoolExecutor)
    phase, against the shared cumulative tracker — the per-call check inside
    _track_cost alone isn't enough there, since parallel section generation
    uses a fresh thread-local tracker per section (merged into the shared one
    only after each section completes), so no single call in that phase would
    ever see the true running total on its own.
    """
    if tracker["usd"] > COST_CEILING_USD:
        raise CostCeilingExceeded(
            f"Run cost ${tracker['usd']:.2f} exceeded the ${COST_CEILING_USD:.2f} ceiling — aborting."
        )


class _Usage:
    """Minimal usage container for non-Anthropic calls (Mistral has different field names)."""
    def __init__(self, input_tokens: int, output_tokens: int):
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cache_creation_input_tokens = 0
        self.cache_read_input_tokens = 0


def _track_cost(tracker: dict, model: str, usage) -> None:
    """Accept an Anthropic usage object (supports cache_creation/cache_read fields)."""
    p = _PRICE.get(model, {"input": 0.0, "output": 0.0})
    # input_tokens is already ONLY non-cached tokens — cache fields are additive, not overlapping
    normal_in   = getattr(usage, "input_tokens", 0) or 0
    cache_write = getattr(usage, "cache_creation_input_tokens", 0) or 0
    cache_read  = getattr(usage, "cache_read_input_tokens", 0) or 0
    output_tok  = getattr(usage, "output_tokens", 0) or 0
    usd = (
        normal_in   * p["input"]          +
        cache_write * p["input"] * 1.25   +
        cache_read  * p["input"] * 0.10   +
        output_tok  * p["output"]
    ) / 1_000_000
    total_in = normal_in + cache_write + cache_read
    tracker["input_tokens"] += total_in
    tracker["output_tokens"] += output_tok
    tracker["usd"] += usd
    tracker["calls"] += 1
    m = tracker["by_model"].setdefault(model, {"calls": 0, "input": 0, "output": 0, "usd": 0.0,
                                               "cache_write": 0, "cache_read": 0})
    m["calls"] += 1
    m["input"] += total_in
    m["output"] += output_tok
    m["usd"] += usd
    m["cache_write"] += cache_write
    m["cache_read"] += cache_read

    check_cost_ceiling(tracker)


# Exponential backoff on transient failures (5xx, connection errors) for every
# Mistral call in the pipeline — applied once here since every call site uses
# this single factory. 3 retries, 500ms-8s backoff, capped at 30s total.
_MISTRAL_RETRY_CONFIG = RetryConfig(
    strategy="backoff",
    backoff=BackoffStrategy(
        initial_interval=500, max_interval=8_000, exponent=2.0, max_elapsed_time=30_000,
    ),
    retry_connection_errors=True,
)


def _mistral_client() -> Mistral:
    return Mistral(api_key=os.environ["MISTRAL_API_KEY"], retry_config=_MISTRAL_RETRY_CONFIG)


# Anthropic's SDK retries transient failures by default (max_retries=2) — bumped
# to 4 here for consistency with the Mistral config above, and made explicit
# rather than relying on the implicit default.
def _anthropic_client() -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"], max_retries=4)
