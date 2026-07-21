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

# A normal main-report run costs ~$0.30-0.50 (see run_log.json); adhoc adds a
# similar amount on adhoc waves. $2.00 gives ~4-6x headroom over the most
# expensive legitimate single-script run while still catching a genuine
# runaway (e.g. an infinite retry loop, or a section stuck re-querying the
# same expensive model) before it burns real money unattended in CI.
MAX_RUN_COST_USD = 2.00


class CostRunawayError(RuntimeError):
    """Raised by _track_cost when a single run's cumulative spend crosses
    MAX_RUN_COST_USD — a hard abort, not a warning, since nothing downstream
    of a runaway cost is worth spending further API budget to finish."""
    pass


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

    if tracker["usd"] > MAX_RUN_COST_USD:
        raise CostRunawayError(
            f"Run cost ${tracker['usd']:.2f} exceeded the ${MAX_RUN_COST_USD:.2f} "
            f"ceiling after a {model} call ({tracker['calls']} calls so far) — aborting."
        )


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
