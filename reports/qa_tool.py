"""
SAFE Data Q&A tool — internal-only CLI for natural-language questions over
SAFE mart data and historical wave narratives (RAG Phase 1).

Offers the model two tools in one agentic loop, reusing
db.run_agentic_query_turns rather than a second copy of the tool-loop
mechanics:

  - query_mart (db.py): SQL over the mart tables, for exact current numbers.
  - search_narrative (this module): cosine-similarity search over past wave
    narrative summaries (narrative_index.py), for qualitative "what did we
    say last wave about X" questions.

Every numeric claim in the final answer is checked against the tool results
actually returned during the conversation BEFORE being printed — this is
blocking even for this internal-only CLI, not deferred as later polish: the
removed /chat chatbot's equivalent check (web/lib/chat/grounding.ts) caught 2
real fabrication bugs live before they reached a user, and that precedent is
the reason this isn't optional.

Usage:
  python reports/qa_tool.py --question "How did the Slovak financing gap change in wave 38?"

Required environment variables:
  MOTHERDUCK_TOKEN, ANTHROPIC_API_KEY, MISTRAL_API_KEY
"""

import argparse
import sys

from cost import _anthropic_client, _track_cost
from db import PROD_SCHEMA, _get_connection, build_mart_catalogue, run_agentic_query_turns
from llm import _NUMBER_RE, _clean_num_token
from narrative_index import retrieve_narrative

SEARCH_NARRATIVE_TOOL = {
    "name": "search_narrative",
    "description": (
        "Search past wave narrative summaries for qualitative context — use this for "
        "'what did we say about X last wave / in the past' style questions, NOT for exact "
        "current numbers (use query_mart for those). Returns the most similar wave "
        "summaries with their wave numbers and a similarity score."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string", "description": "What to search for, e.g. 'AI adoption trends'"},
        },
        "required": ["query"],
    },
}

QA_SYSTEM_TEMPLATE = """You are a data analyst answering questions about the ECB SAFE survey \
(Survey on Access to Finance of Enterprises) for Slovakia. You have two tools:

- query_mart: run a read-only SQL SELECT against the mart tables for exact current numbers.
- search_narrative: search past wave narrative summaries for qualitative context (\
"what did we say last wave about X").

Always use a tool before answering — never answer from memory or estimate a number. Cite \
every number in your answer exactly as it appears in a tool result. If a tool result doesn't \
contain the number you need, say you don't know rather than guessing or approximating.

{schema_catalogue}
"""

# Numbers this short (waves, list positions, small counts) are too generic to
# usefully ground — checking them produces noise, not signal (mirrors the
# n<=1-digit skip llm.py's _check_numeric_grounding effectively applies via
# its own marker-based filtering, adapted here since qa_tool has no fixed
# value_cols/DataFrame to check against, only raw tool-result text).
_MIN_GROUNDED_NUMBER_LEN = 2


def _search_narrative_tool_result(query: str, con) -> str:
    results = retrieve_narrative(query, con)
    if not results:
        return "No narrative index found — run reports/narrative_index.py to build it first."
    return "\n".join(
        f"Wave {r['wave_number']} (similarity {r['similarity']:.2f}): {r['notable_summary']}"
        for r in results
    )


def _check_answer_grounding(answer: str, source_texts: list[str]) -> list[str]:
    """Return one error string per number cited in `answer` that doesn't appear
    in any of source_texts (the tool results actually returned this
    conversation). Mirrors llm.py's _check_numeric_grounding, but works over
    raw tool-result text (markdown tables, narrative prose) instead of a
    DataFrame + value_cols — qa_tool's questions aren't tied to one section's
    fixed config, so there's no equivalent structured source to check against."""
    combined = "\n".join(source_texts)
    source_numbers = {_clean_num_token(m.group(1)) for m in _NUMBER_RE.finditer(combined)}
    errors = []
    for m in _NUMBER_RE.finditer(answer):
        num_str = _clean_num_token(m.group(1))
        if len(num_str) < _MIN_GROUNDED_NUMBER_LEN:
            continue
        if num_str not in source_numbers:
            errors.append(f"'{num_str}' not found in any tool result this conversation")
    return errors


def ask(question: str, con=None, client=None, cost_tracker: dict | None = None) -> dict:
    """Answer `question` via the agentic query_mart + search_narrative loop.

    Returns {"answer": str, "tool_calls": int, "grounding_errors": [str, ...]}.
    grounding_errors is non-empty iff the answer cited an unverifiable number —
    callers (main(), tests) decide how to react; this function always returns
    rather than raising, so a grounding failure is visible either way.
    """
    owns_con = con is None
    if owns_con:
        con = _get_connection()
    if client is None:
        client = _anthropic_client()
    if cost_tracker is None:
        cost_tracker = {"input_tokens": 0, "output_tokens": 0, "usd": 0.0, "calls": 0, "by_model": {}}

    try:
        mart_catalogue = build_mart_catalogue(con, PROD_SCHEMA)
        system_prompt = QA_SYSTEM_TEMPLATE.format(schema_catalogue=mart_catalogue)
        cached_system = [{"type": "text", "text": system_prompt, "cache_control": {"type": "ephemeral"}}]
        messages = [{"role": "user", "content": question}]

        def _handle_extra_tool(name: str, tool_input: dict) -> str:
            if name == "search_narrative":
                return _search_narrative_tool_result(tool_input.get("query", ""), con)
            return f"ERROR: unknown tool {name!r}"

        messages, tool_calls = run_agentic_query_turns(
            client, cached_system, messages, con, PROD_SCHEMA, cost_tracker,
            extra_tools=[SEARCH_NARRATIVE_TOOL], extra_tool_handler=_handle_extra_tool,
        )

        # Every tool_result (query_mart's and search_narrative's alike) is now
        # in the turn history — pull them all out as the grounding source pool.
        source_texts: list[str] = []
        for msg in messages:
            if msg.get("role") == "user" and isinstance(msg.get("content"), list):
                for block in msg["content"]:
                    if isinstance(block, dict) and block.get("type") == "tool_result":
                        source_texts.append(str(block.get("content", "")))

        messages.append({
            "role": "user",
            "content": "Now answer the question in plain text, citing numbers exactly as "
                       "they appeared in the tool results above.",
        })
        final = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            system=cached_system,
            messages=messages,
            extra_headers={"anthropic-beta": "prompt-caching-2024-07-31"},
        )
        _track_cost(cost_tracker, "claude-sonnet-4-6", final.usage)
        answer = " ".join(b.text for b in final.content if hasattr(b, "text")).strip()

        grounding_errors = _check_answer_grounding(answer, source_texts)
        return {"answer": answer, "tool_calls": tool_calls, "grounding_errors": grounding_errors}
    finally:
        if owns_con:
            con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="SAFE data Q&A tool (internal-only, RAG Phase 1)")
    parser.add_argument("--question", type=str, required=True)
    args = parser.parse_args()

    result = ask(args.question)

    if result["grounding_errors"]:
        print("BLOCKED — answer cites unverifiable number(s), not printing:")
        for e in result["grounding_errors"]:
            print(f"  - {e}")
        sys.exit(1)

    print(result["answer"])


if __name__ == "__main__":
    main()
