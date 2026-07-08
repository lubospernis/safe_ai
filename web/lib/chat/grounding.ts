// Numeric grounding check for the chat agent's final answer. Unlike
// reports/llm.py::_check_numeric_grounding (monitoring-only — the report
// pipeline has a human/quality-gate review downstream), this check BLOCKS:
// there is no review step between this chatbot and the analyst reading it, so
// a fabricated number must not ship. Simpler than the Python version — checks
// against the full text of every tool result run this session, not a specific
// DataFrame's value columns, since chat questions aren't scoped to one table.

const NUMBER_RE = /(?<!\d)(\d+(?:\.\d+)?)(?!\d)/g;

// A bare single-digit INTEGER (no decimal point) is almost always a wave
// number, rank, or count rather than a cited data value — skip only those
// (same reasoning as reports/llm.py's skip list). Decimals are never skipped
// by magnitude: a value like "5.28" is exactly the shape a fabricated rate or
// net balance takes, and is the case that matters most to catch.
const SKIP_INTEGER_BELOW = 10;
const SKIP_ABOVE = 100000;

const decimalPlaces = (numStr: string): number => {
  const i = numStr.indexOf(".");
  return i === -1 ? 0 : numStr.length - i - 1;
};

// Matches "scale of 1 to 10", "1-10 scale", "scale from 1 to 10", etc. — these
// describe the measurement scale (e.g. Q0B pressingness is rated 1-10), not a
// cited data value, but the bare numbers inside the phrase still match
// NUMBER_RE and would otherwise be flagged as ungrounded since a tool result
// table of labels+scores rarely contains the literal scale endpoints.
const SCALE_PHRASE_RE = /\bscale\s+(?:of|from)?\s*\d+\s*(?:to|-|–)\s*\d+|\b\d+\s*(?:to|-|–)\s*\d+\s*scale\b/gi;

function findScaleDescriptionRanges(text: string): Array<[number, number]> {
  const ranges: Array<[number, number]> = [];
  for (const m of text.matchAll(SCALE_PHRASE_RE)) {
    const start = m.index ?? 0;
    ranges.push([start, start + m[0].length]);
  }
  return ranges;
}

function extractNumbers(text: string): Set<string> {
  const nums = new Set<string>();
  for (const m of text.matchAll(NUMBER_RE)) {
    const numStr = m[1];
    nums.add(numStr);
    // Only add a "X.0" variant for whole numbers (so "31" matches a grounded
    // "31.0") — never coarsen an already-decimal value's precision, or e.g.
    // "5.28" and "5.31" would collapse to the same "5.3" bucket and falsely
    // match each other, defeating the check for exactly the 2-decimal-place
    // values (rates, net balances) that matter most to verify.
    if (decimalPlaces(numStr) === 0) nums.add(`${numStr}.0`);
  }
  return nums;
}

/** Returns the numbers in answerText that don't appear anywhere in the
 * accumulated tool-result text — i.e. likely fabricated. Empty = fully grounded. */
export function checkNumericGrounding(answerText: string, toolResultsText: string): string[] {
  const groundedNumbers = extractNumbers(toolResultsText);
  const ungrounded: string[] = [];
  const scaleRanges = findScaleDescriptionRanges(answerText);

  for (const m of answerText.matchAll(NUMBER_RE)) {
    const numStr = m[1];
    const value = parseFloat(numStr);
    if (Number.isNaN(value) || value > SKIP_ABOVE) continue;
    const isInteger = !numStr.includes(".");
    if (isInteger && value < SKIP_INTEGER_BELOW) continue;

    const start = m.index ?? 0;

    // Sample-size citation "n=62" / "(n=62)" — not a data value being asserted.
    const preceding = answerText.slice(Math.max(0, start - 3), start);
    if (/n\s*=\s*$/.test(preceding)) continue;

    // Wave reference: "wave 38", "wave 37" — the wave number itself is a filter
    // criterion echoed back in prose, not a data value that needs to appear in
    // the tool result's SELECT columns (a query can filter WHERE wave_number = 38
    // without ever projecting wave_number into the output). Ported from
    // reports/llm.py::_check_numeric_grounding, which has this exact skip and
    // was the source of a real false positive here before this was added.
    const precedingWord = answerText.slice(Math.max(0, start - 6), start);
    if (/wave\s*$/i.test(precedingWord)) continue;

    // Pressingness-scale denominator: "6.19/10" or "6.19 out of 10" — the "10"
    // is the fixed scale denominator, not a separately-cited data value.
    if (start > 0 && answerText[start - 1] === "/") continue;
    if (/out\s+of\s*$/i.test(answerText.slice(Math.max(0, start - 10), start))) continue;

    // "on a scale of 1 to 10" — describes the measurement scale, not a cited value.
    if (scaleRanges.some(([rStart, rEnd]) => start >= rStart && start < rEnd)) continue;

    const variant = decimalPlaces(numStr) === 0 ? `${numStr}.0` : numStr;
    if (!groundedNumbers.has(numStr) && !groundedNumbers.has(variant)) {
      ungrounded.push(numStr);
    }
  }

  return [...new Set(ungrounded)];
}
