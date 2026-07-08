import { describe, expect, it } from "vitest";
import { checkNumericGrounding } from "./grounding";

const toolResult = `| wave_number | survey_period_label | country_code | rejection_rate_wtd |
| --- | --- | --- | --- |
| 37 | 2025Q4 | SK | 5.31 |`;

describe("checkNumericGrounding", () => {
  it("passes when all cited numbers appear in the tool result", () => {
    const answer = "Slovakia's rejection rate in wave 37 (2025Q4) was 5.31%.";
    expect(checkNumericGrounding(answer, toolResult)).toEqual([]);
  });

  it("flags a number that was never actually queried", () => {
    const answer = "Slovakia's rate was 5.31%, compared to the euro area's 5.28%.";
    const ungrounded = checkNumericGrounding(answer, toolResult);
    expect(ungrounded).toContain("5.28");
  });

  it("does not flag wave numbers or small counts", () => {
    const answer = "In wave 37, 8 firms reported this.";
    expect(checkNumericGrounding(answer, toolResult)).toEqual([]);
  });

  it("does not flag a sample-size citation", () => {
    const answer = "Slovakia's rate was 5.31% (n=237).";
    expect(checkNumericGrounding(answer, toolResult)).toEqual([]);
  });

  it("does not flag scale-description numbers like 'scale of 1 to 10'", () => {
    // Regression test: the tool result table only has problem_label +
    // avg_pressingness_wtd columns — the literal "1" and "10" describing the
    // measurement scale never appear in it, but they aren't fabricated data
    // values either. A prior bug here caused a genuinely grounded 6.19 answer
    // to be discarded as "unverifiable" because of this exact phrasing.
    const businessProblemsResult = `| problem_label | avg_pressingness_wtd |
| --- | --- |
| Costs of production or labour | 6.19 |`;
    const answer =
      "On a scale of 1 to 10, Slovak firms rated Costs of production or labour as the most pressing business problem, with an average score of 6.19.";
    expect(checkNumericGrounding(answer, businessProblemsResult)).toEqual([]);
  });

  it("still flags a fabricated number even when a scale phrase is present elsewhere", () => {
    const businessProblemsResult = `| problem_label | avg_pressingness_wtd |
| --- | --- |
| Costs of production or labour | 6.19 |`;
    const answer =
      "On a scale of 1 to 10, Slovak firms rated Costs of production or labour as the most pressing problem at 6.19, compared to competition at 4.75.";
    const ungrounded = checkNumericGrounding(answer, businessProblemsResult);
    expect(ungrounded).toContain("4.75");
  });

  it("does not flag a wave number mentioned in prose when the query never projected wave_number", () => {
    // Regression test: a real production bug. The tool result table here has
    // NO wave_number column at all (the query filtered WHERE wave_number = 38
    // without selecting it) — "wave 38" in the answer text was being flagged
    // as an ungrounded number, discarding an otherwise fully-grounded answer.
    const businessProblemsResult = `| problem_label | avg_pressingness_wtd |
| --- | --- |
| Costs of production or labour | 6.19 |`;
    const answer =
      "The biggest problem for Slovak firms in wave 38 is Costs of production or labour, with an average pressingness score of 6.19.";
    expect(checkNumericGrounding(answer, businessProblemsResult)).toEqual([]);
  });

  it("does not flag the '10' in 'rated X out of 10' phrasing", () => {
    const businessProblemsResult = `| problem_label | avg_pressingness_wtd |
| --- | --- |
| Costs of production or labour | 6.19 |`;
    const answer = "Costs of production or labour was rated 6.19 out of 10.";
    expect(checkNumericGrounding(answer, businessProblemsResult)).toEqual([]);
  });
});
