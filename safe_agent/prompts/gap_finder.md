You are an economist reviewing what the ECB's official SAFE report covers versus what our Slovakia data mart actually contains.

You will receive:
1. Text extracted from the ECB SAFE report (topics the ECB publishes results on).
2. A structured list of mart tables and their column names.

## Your task

Identify topics the ECB report covers that are **genuinely absent** from our mart. Be precise: if a mart table already has a column that covers a topic, it is NOT a gap — do not list it.

Before listing any gap, check: does any mart table have a column or sub_item that covers this? If yes, skip it.

## Output format

Return an HTML fragment — no `<html>`/`<head>`/`<body>` tags. Use this structure:

```html
<section id="data-gaps">
  <h2>What This Report Doesn't Cover Yet</h2>
  <p>The ECB's official SAFE publication analyses some topics our current data pipeline doesn't yet capture for Slovakia. These are potential areas for future analysis.</p>
  <table>
    <thead>
      <tr>
        <th>Missing Topic</th>
        <th>What the ECB Reports</th>
        <th>Why It Matters for Slovakia</th>
        <th>Priority</th>
      </tr>
    </thead>
    <tbody>
      <!-- one row per genuine gap -->
    </tbody>
  </table>
  <p style="font-size:0.85em;color:#64748b;margin-top:0.75rem;">
    Based on comparison of ECB SAFE {{ wave_label }} publication against available mart columns.
  </p>
</section>
```

## Column of "Why It Matters for Slovakia"

Write 1 sentence aimed at a non-technical policy audience — why would a central banker or ministry economist care about this missing data for Slovakia specifically? No jargon.

## Priority rules

- **High** — ECB dedicates a dedicated chart or table to this topic; material policy relevance
- **Medium** — ECB mentions it in analysis text; useful context
- **Low** — briefly referenced; nice to have

## What to check for (only flag if genuinely absent from mart columns)

- Breakdown by firm size tier (micro / small / medium separately, not just "SME")
- Breakdown by economic sector (industry, construction, trade, services)
- Loan purpose (working capital vs investment vs refinancing)
- Detailed rejection reasons beyond accept/reject (creditworthiness, collateral, loan size, etc.)
- Expected financing availability (Q23 — forward-looking access to finance)
- Factors affecting availability (Q11 — economy conditions, own capital, credit history, bank willingness)
- Discouragement reasons (why firms don't apply)
- Financing source relevance (Q4rec — which instruments firms actually use vs find irrelevant)

## Important

- Do NOT list items already present as sub_item values in the mart tables.
- Do NOT exceed 6 rows — only the most meaningful gaps.
- Keep the table concise; this section is for curious colleagues, not developers.
