import { buildMartTool, type BuiltMartTool } from "./martToolBuilder";
import { MART_TOOL_SPECS } from "./martToolSpecs";

export const STRUCTURED_MART_TOOLS: BuiltMartTool[] = MART_TOOL_SPECS.map(buildMartTool);

export function findMartTool(toolName: string): BuiltMartTool | undefined {
  return STRUCTURED_MART_TOOLS.find((t) => t.tool.function.name === toolName);
}
