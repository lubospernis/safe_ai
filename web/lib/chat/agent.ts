// Mistral-native agentic loop: translates a natural-language question into SQL
// against the SAFE mart tables, executes it, and returns a grounded answer.
// Redesigned from reports/llm.py::get_section_content_agentic for Mistral's
// tool-calling shape (@mistralai/mistralai's `toolCalls`/`toolChoice`, not
// Anthropic's content-block tool_use). Confirmed against the installed SDK
// (@mistralai/mistralai 2.4.1): ChatCompletionRequest.toolChoice accepts either
// ToolChoiceEnum ("auto"/"any"/"required"/"none") or a ToolChoice naming one
// specific function — used below to force the final emit_answer call.

import { Mistral } from "@mistralai/mistralai";
import type { ChatCompletionRequest, Tool, ToolCall } from "@mistralai/mistralai/models/components";
import { runQueryTool, fetchTableForDisplay, type QueryToolResult } from "./duckdb";
import { newCostTracker, trackCost, type CostTracker } from "./cost";
import { MART_QUERY_TEMPLATES } from "./martQueryTemplates";
import { checkNumericGrounding } from "./grounding";
import { STRUCTURED_MART_TOOLS, findMartTool } from "./tools";
import type { DuckDBConnection } from "@duckdb/node-api";

const UNVERIFIABLE_ANSWER =
  "I couldn't verify a grounded answer to that question from the available data — please try rephrasing or asking about one metric at a time.";

const MODEL = "mistral-large-2512";
const MAX_TOOL_TURNS = 2;

// Fallback tool for anything the structured mart tools (STRUCTURED_MART_TOOLS,
// built from web/lib/chat/tools/martToolSpecs.ts) don't cover — pre-wave-30
// history, unusual cross-mart questions, or a mart without a dedicated tool
// yet. Prefer a structured tool whenever one exists for the mart in question:
// it builds pre-verified SQL itself, so the model can't misname a column the
// way it repeatedly did when this was the only tool available.
const QUERY_MART_TOOL: Tool & { type: "function" } = {
  type: "function",
  function: {
    name: "query_mart",
    description:
      "Execute a read-only DuckDB SELECT against the SAFE mart tables. Only use this when no " +
      "structured tool covers the mart you need — e.g. pre-wave-30 history (int_safe__core_questions_long), " +
      "or a mart without a dedicated tool. Do NOT use this for business problems or financing " +
      "conditions/need/availability/terms — use get_business_problems or get_financing_conditions instead. " +
      "Do NOT use this to discover table or column names — see the schema catalogue in the system prompt. " +
      "Always use fully-qualified names: main_safe.mart_safe__<name>. Only SELECT is permitted.",
    parameters: {
      type: "object",
      properties: {
        sql: {
          type: "string",
          description:
            "A SELECT query. Must reference main_safe.mart_safe__* or main_safe.int_safe__core_questions_long only.",
        },
      },
      required: ["sql"],
    },
  },
};

const EMIT_ANSWER_TOOL: Tool & { type: "function" } = {
  type: "function",
  function: {
    name: "emit_answer",
    description: "Emit the final answer to the user's question as structured output.",
    parameters: {
      type: "object",
      properties: {
        answer_text: {
          type: "string",
          description: "1-4 sentence plain-English answer, citing only numbers from tool results actually run.",
        },
        table_columns: {
          type: "array",
          items: { type: "string" },
          description: "Column names for the data table backing the answer, if a query was run.",
        },
      },
      required: ["answer_text"],
    },
  },
};

function systemPrompt(catalogue: string): string {
  return `You are an analyst answering ad hoc questions about ECB SAFE survey data for Slovakia, using tools to fetch real data from MotherDuck.

CRITICAL DATA RULE:
Only cite numbers that appear verbatim in a tool result you actually got back in this session. Do NOT invent, estimate, or paraphrase percentages, net balances, or counts. If you cannot find the data to answer the question, say so plainly rather than guessing.
If the question compares two or more groups (e.g. Slovakia vs euro area, two waves, two countries), your query MUST return a row for EVERY group being compared — never state a value for a group you did not actually fetch, even if a similar-looking number appeared in a query for a different group. A comparison with one side unverified must say so rather than filling in the missing side.

Default filters unless the question says otherwise: countries SK, EA, DE; firmSize "all".

Tool selection:
- Prefer a structured tool (get_business_problems, get_financing_conditions, etc.) whenever the
  question is about that tool's metric — it takes typed arguments (countries, wave range, firm size,
  and a dimension code where relevant) and builds correct SQL itself. You never need to know column
  names for a mart that has a structured tool.
- Only fall back to query_mart for marts with no structured tool, or pre-wave-30 history
  (int_safe__core_questions_long). See the schema catalogue below for what query_mart covers.

Available mart tables and columns for query_mart (marts with a structured tool are not listed
here — use the structured tool instead):
${catalogue}

Query templates for query_mart (fill in UPPER_CASE placeholders only):
${MART_QUERY_TEMPLATES}

Process:
1. Call the tool(s) needed to fetch the data for the question (at most a couple of turns).
2. Once you have what you need, call emit_answer with a concise, grounded answer.
Do not call query_mart to explore table/column names — the catalogue above is complete.
Do not call emit_answer until you've either fetched the data you need or determined the question can't be answered from these tables.`;
}

export interface ChatTurn {
  question: string;
  answerText: string;
}

export interface AgentResult {
  answerText: string;
  sql: string | null;
  table: { columns: string[]; rows: unknown[][] } | null;
  cost: CostTracker;
}

function parseToolArgs(toolCall: ToolCall): Record<string, unknown> {
  const raw = toolCall.function.arguments;
  if (typeof raw === "string") {
    try {
      return JSON.parse(raw);
    } catch {
      return {};
    }
  }
  return raw ?? {};
}

export async function runChatAgent(
  question: string,
  history: ChatTurn[],
  con: DuckDBConnection,
  catalogue: string,
  mistralClient: Mistral,
): Promise<AgentResult> {
  const cost = newCostTracker();
  const messages: ChatCompletionRequest["messages"] = [
    { role: "system", content: systemPrompt(catalogue) },
  ];
  // Replay prior turns as plain text — tool calls from earlier turns aren't
  // replayed, keeping the message history small and cheap.
  for (const turn of history) {
    messages.push({ role: "user", content: turn.question });
    messages.push({ role: "assistant", content: turn.answerText });
  }
  messages.push({ role: "user", content: question });

  let lastExecutedSql: string | null = null;
  const toolResultsText: string[] = [];

  const availableTools: (Tool & { type: "function" })[] = [
    ...STRUCTURED_MART_TOOLS.map((t) => t.tool),
    QUERY_MART_TOOL,
  ];

  for (let turn = 0; turn < MAX_TOOL_TURNS; turn++) {
    const response = await mistralClient.chat.complete({
      model: MODEL,
      maxTokens: 600,
      tools: availableTools,
      messages,
    });
    trackCost(cost, MODEL, response.usage);

    const choice = response.choices[0];
    const toolCalls = choice.message?.toolCalls;
    if (!toolCalls || toolCalls.length === 0) {
      // Model didn't request a tool this turn — nothing more to fetch, move to forcing an answer.
      break;
    }

    messages.push({
      role: "assistant",
      content: choice.message?.content ?? "",
      toolCalls,
    });

    for (const call of toolCalls) {
      const args = parseToolArgs(call);
      const structuredTool = findMartTool(call.function.name);

      let result: QueryToolResult & { sql?: string };
      if (structuredTool) {
        result = await structuredTool.run(args, con);
      } else if (call.function.name === "query_mart") {
        const sql = typeof args.sql === "string" ? args.sql : "";
        result = { ...(await runQueryTool(sql, con)), sql };
      } else {
        // Unrecognized tool name — Mistral requires a response for every tool
        // call in the same turn, so this must still get a tool-role message
        // rather than being silently dropped.
        result = { ok: false, error: `ERROR: unknown tool "${call.function.name}"` };
      }

      if (result.ok) {
        lastExecutedSql = result.sql ?? null;
        toolResultsText.push(result.markdown);
      }
      messages.push({
        role: "tool",
        toolCallId: call.id,
        name: call.function.name,
        content: result.ok ? result.markdown : result.error,
      });
    }
  }

  const groundedText = toolResultsText.join("\n\n");

  // Force the final answer via a specifically-named tool choice — Mistral's
  // ToolChoice type (distinct from the auto/any/required ToolChoiceEnum) lets us
  // name emit_answer directly, which is stricter than Anthropic's generic "any".
  // Blocking grounding check + one retry: this chatbot has no downstream human
  // review (unlike reports/llm.py's monitoring-only _check_numeric_grounding),
  // so a fabricated number must not reach the analyst.
  let answerText = "Sorry, I couldn't produce an answer for that question.";
  for (let attempt = 0; attempt < 2; attempt++) {
    if (attempt === 0) {
      messages.push({ role: "user", content: "Now call emit_answer with your final response." });
    } else {
      messages.push({
        role: "user",
        content:
          `Your previous answer cited number(s) not found in any query result this session: ${answerText}. ` +
          "Revise your answer to only cite numbers from the tool results above, or state that part of the " +
          "comparison could not be verified. Call emit_answer again.",
      });
    }

    const emitResponse = await mistralClient.chat.complete({
      model: MODEL,
      maxTokens: 400,
      tools: [EMIT_ANSWER_TOOL],
      toolChoice: { type: "function", function: { name: "emit_answer" } },
      messages,
    });
    trackCost(cost, MODEL, emitResponse.usage);

    const emitCall = emitResponse.choices[0].message?.toolCalls?.[0];
    let candidate = "";
    if (emitCall) {
      const args = parseToolArgs(emitCall);
      if (typeof args.answer_text === "string" && args.answer_text.trim()) {
        candidate = args.answer_text.trim();
      }
      // Mistral requires every tool call in an assistant message to have a
      // matching tool-role response before the next request — append both, or
      // a retry's follow-up user message leaves this one dangling and the API
      // rejects the next call with "Not the same number of function calls and responses".
      messages.push({
        role: "assistant",
        content: emitResponse.choices[0].message?.content ?? "",
        toolCalls: emitResponse.choices[0].message?.toolCalls ?? [],
      });
      messages.push({ role: "tool", toolCallId: emitCall.id, name: emitCall.function.name, content: "ok" });
    }
    if (!candidate) break;

    const ungrounded = checkNumericGrounding(candidate, groundedText);
    if (ungrounded.length === 0) {
      answerText = candidate;
      break;
    }
    answerText = candidate; // kept only for the retry's correction message
    if (attempt === 1) {
      // Still ungrounded after one retry — don't ship a possibly-fabricated number.
      answerText = UNVERIFIABLE_ANSWER;
    }
  }

  const table = lastExecutedSql ? await fetchTableForDisplay(lastExecutedSql, con) : null;

  return { answerText, sql: lastExecutedSql, table, cost };
}
