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
});
