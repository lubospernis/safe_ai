// Cost tracking for the chat agent, trimmed to only the models this feature can
// invoke (Mistral — no Anthropic calls in this loop). Ported from reports/cost.py.
// Source of truth: reports/cost.py — keep in sync manually, no automated sync exists.

const PRICE: Record<string, { input: number; output: number }> = {
  "mistral-large-2512": { input: 2.0, output: 6.0 },
};

export interface CostTracker {
  inputTokens: number;
  outputTokens: number;
  usd: number;
  calls: number;
}

export function newCostTracker(): CostTracker {
  return { inputTokens: 0, outputTokens: 0, usd: 0, calls: 0 };
}

// Mistral's UsageInfo has no prompt-caching concept (unlike Anthropic's usage
// object, which reports.cost.py::_track_cost also handles) — so this is a
// simpler formula than the Python version, not because caching doesn't matter,
// but because there's nothing to cache here (YAGNI: no dead cache-rate terms).
export function trackCost(
  tracker: CostTracker,
  model: string,
  usage: { promptTokens?: number; completionTokens?: number } | undefined,
): void {
  const price = PRICE[model] ?? { input: 0, output: 0 };
  const inputTokens = usage?.promptTokens ?? 0;
  const outputTokens = usage?.completionTokens ?? 0;
  const usd = (inputTokens * price.input + outputTokens * price.output) / 1_000_000;
  tracker.inputTokens += inputTokens;
  tracker.outputTokens += outputTokens;
  tracker.usd += usd;
  tracker.calls += 1;
}
