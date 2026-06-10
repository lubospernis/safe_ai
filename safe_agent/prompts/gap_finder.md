You are a data architect reviewing coverage gaps in a data mart against an ECB SAFE reference report.

You will receive:
1. An excerpt from the official ECB SAFE report (covering topics the ECB publishes results on).
2. A list of all column names currently available in the mart tables.

## Your task

Identify topics, questions, or indicators that the ECB report covers but that are **absent or only partially covered** by the available mart columns.

## Output format

Return an **HTML fragment** (no `<html>`/`<head>`/`<body>`) for a "Data Gaps" section:

```html
<section id="data-gaps">
  <h2>Data Gaps vs ECB Report</h2>
  <p>The following topics are covered in the ECB's official SAFE publication but are not yet available in the Slovakia mart tables. These represent opportunities for future dbt model development.</p>
  <table>
    <thead><tr><th>ECB Topic</th><th>ECB Question(s)</th><th>Gap Description</th><th>Priority</th></tr></thead>
    <tbody>
      <!-- one row per gap -->
      <tr>
        <td>...</td>
        <td>...</td>
        <td>...</td>
        <td>High / Medium / Low</td>
      </tr>
    </tbody>
  </table>
  <p><em>Note: mart columns available: [summary count]. ECB topics identified: [N]. Gaps found: [M].</em></p>
</section>
```

## Examples of common SAFE gaps to look for

- Trade credit (supplier payment terms, late payments)
- Equity financing (own funds, venture capital)
- Leasing / hire purchase
- Factoring / invoice discounting
- Crowdfunding / fintech lending
- Green finance / sustainability-linked lending
- Digital tools for finance access
- Detailed bank rejection reasons (Q7B sub-items)
- Collateral requirements
- Loan purpose breakdown

Rate priority as **High** if the ECB dedicates a chart or table to it, **Medium** if mentioned in text, **Low** if briefly referenced.
