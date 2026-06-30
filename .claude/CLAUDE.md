## Roadmap

Feature ideas and implementation status are tracked in `.claude/ROADMAP.md`.
When you complete a task that corresponds to a roadmap item, tick it off (change `[ ]` to `[x]`
and move it to the Done section). Do not add speculative sub-tasks — only mark things done
when the code is actually shipped.

---

## Testing Without MotherDuck

Use the **dev target** (local DuckDB) for all testing — no `MOTHERDUCK_TOKEN` needed.

```bash
# 1. Rebuild dev marts after any dbt model change
cd dbt_project && ../env/bin/dbt run --profiles-dir . --target dev

# 2. Run the report against local DuckDB
cd ..
source .env && env/bin/python3 reports/run_report.py --dev
```

**API keys for local testing**: stored in `.env` (gitignored). `source .env` loads
`ANTHROPIC_API_KEY` into the shell before running the report script.

**Dev vs prod schema**: dbt's dev target writes to `main_safe_safe` (DuckDB file: `dev.duckdb`);
prod writes to `main_safe` on MotherDuck. `run_report.py --dev` rewrites the schema name
automatically — SQL files always reference `main_safe.*`.

**Python env**: use `env/bin/python3` (the project venv). matplotlib is installed there.

---

## SAFE Survey Reference Files

CRITICAL: Before writing ANY descriptions, labels, or mappings for survey variables
(question IDs, answer codes, sub-items), you MUST read the annex first. Do NOT rely
on prior knowledge — the actual codes differ from common assumptions.

1. **Authoritative question text, sub-item labels, and answer codes**:
   `/Users/lubospernis/Documents/safe_ai/collateral/annex.csv`

2. **Questionnaire (how it looks to respondents)**:
   https://www.ecb.europa.eu/stats/accesstofinancesofenterprises/pdf/questionnaire/ecb.safeq202602.en.pdf

3. **User guide / methodology**:
   https://www.ecb.europa.eu/stats/pdf/surveys/sme/ecb.safemi.en.pdf

---

## Database

- MotherDuck database: `my_db`, schema `main_safe`
- All mart tables cover **wave 30 (2024Q1) onward**, three-month reference period only
- All mart tables are pre-aggregated to **wave × country** (and sub-item/question where applicable)
- dbt profile: `safe_ai`, target `prod`

---

## Mart Catalogue

### Which mart to use

| Goal | Mart |
|---|---|
| Financing needs/availability/terms (ECB methodology) | `mart_safe__financing_conditions` |
| Financing purpose breakdown by country (Q6A) | `mart_safe__financing_purpose` |
| Headline Slovakia KPIs (pre-selected, AI-ready) | `mart_safe__slovakia_kpis` |
| Pressing business problems (Q0B pressingness scores) | `mart_safe__business_problems` |
| Factors affecting access to financing (Q11) | `mart_safe__financing_factors` |
| Loan application rates, discouragement, rejection | `mart_safe__loan_applications` |
| Business situation (turnover, profit, labour costs, etc.) | `mart_safe__business_situation` |
| Expected changes in turnover and investment (Q26) | `mart_safe__outlook` |
| Expected availability of external financing (Q23) | `mart_safe__availability_expectations` |
| Inflation expectations and risk direction (Q31/Q33/Q34) | `mart_safe__expectations` |
| Firm counts and SME/large composition per wave × country | `mart_safe__survey_participants` |
| Response rates per question × sub-item × country | `mart_safe__question_coverage` |

---

### mart_safe__financing_conditions

ECB-methodology net balances for Q5 (financing need), Q9 (availability), Q10 (bank loan terms).
Three-month reference period only. Wave 30 (2024Q1) onward.

**Key rules**:
- `firm_size`: `'all'` = ECB-comparable (all firms), `'sme'` = bands 1–3, `'large'` = band 4
- Non-response codes 7 (N/A) and 9 (DK) excluded from denominator
- Rounded to 1 dp; ECB publishes integers — ±1pp residual expected due to weight precision
- `financing_gap_wtd` on Q9 rows = Q5.net_balance − Q9.net_balance (pre-computed; do not re-derive)
- Q10 net_balance positive = bank **tightened** terms (adverse for firms)

**Instruments**: a, b, c, d, f, g, h (Q5/Q9); a–f (Q10)

```sql
-- ECB Table 1 equivalent for Slovakia Q1 2026
SELECT question_id, sub_item_label, net_balance_wtd
FROM main_safe.mart_safe__financing_conditions
WHERE country_code = 'SK' AND wave_number = 38
  AND question_id IN ('q5', 'q9') AND sub_item IN ('a', 'f')
  AND firm_size = 'all'
ORDER BY question_id, sub_item
```

---

### mart_safe__financing_purpose

Weighted % of firms citing each purpose for financing (Q6A multi-select).
Three-month reference period only. Wave 30 (2024Q1) onward.

**Key rules**:
- Multi-select: each purpose is independent; percentages across purposes can sum to >100%
- `pct_cited_wtd` = share of valid respondents (codes 1 or 2) who cited the purpose (code=1)
- DK/NA (code 99) and null excluded from denominator
- `firm_size`: `'all'` = all firms, `'sme'` = bands 1–3, `'large'` = band 4
- For regional comparison, filter `country_code IN ('SK','CZ','HU','PL','AT','DE')`

**Purposes** (purpose_id): 1=Fixed investment, 2=Inventory and working capital,
3=Hiring and training, 4=New products/services, 5=Refinancing, 6=Other

```sql
-- Slovakia vs neighbours, latest wave, SMEs only
SELECT country_code, purpose_label, pct_cited_wtd, n_respondents
FROM main_safe.mart_safe__financing_purpose
WHERE country_code IN ('SK','CZ','HU','PL','AT','DE')
  AND wave_number = 38 AND firm_size = 'sme'
ORDER BY country_code, purpose_id
```

---

### mart_safe__slovakia_kpis

One row per wave, all headline KPIs for Slovakia pre-joined and named.
**Always use this for Slovakia trend analysis** — do not recompute from raw marts.

Columns include: `q5a_need_nb`, `q9a_avail_nb`, `bank_loan_gap`, `q10a_interest_nb`,
`turnover_nb`, `profit_nb`, `labour_cost_nb`, `employees_nb`, `investment_nb`,
`press_*` (7 pressingness scores), `bank_loan_app_rate`, `bank_loan_disc_rate`,
`bank_loan_rej_rate`, `bank_loan_access_gap`, `turnover_outlook_nb`, `investment_outlook_nb`.

```sql
SELECT * FROM main_safe.mart_safe__slovakia_kpis ORDER BY wave_number DESC LIMIT 5
```

---

### mart_safe__business_problems

Weighted average pressingness scores (scale 1–10) for 7 business problems by country × wave.
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.

**Key rules**:
- `avg_pressingness_wtd` is a **score 1–10**, NOT a net balance or percentage
- problem_id: 1=Finding customers, 2=Competition, 3=Access to finance,
  4=Costs of production/labour, 5=Skilled staff, 6=Regulation, 7=Other

```sql
SELECT problem_label, avg_pressingness_wtd
FROM main_safe.mart_safe__business_problems
WHERE country_code = 'SK' AND wave_number = 38 AND firm_size = 'all'
ORDER BY avg_pressingness_wtd DESC
```

---

### mart_safe__financing_factors

Net balances for Q11 (factors affecting access to external financing) by country × wave.
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.

**Key rules**:
- Positive = factor improved (FAVOURABLE); negative = deteriorated (ADVERSE)
- sub_item f = willingness of banks (key credit supply signal)

---

### mart_safe__loan_applications

Application rates, discouragement, rejection rates from Q7A/Q7B.
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.

Key columns: `application_rate_wtd`, `discouragement_rate_wtd`, `rejection_rate_wtd`,
`financing_gap_wtd` (= discouragement + rejection share of all respondents — ECB headline access indicator).

`sub_item = 'a'` = bank loans (main instrument of interest).

---

### mart_safe__business_situation

Net balances for Q2 (business situation indicators).
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.
sub_items: a=Turnover, b=Labour costs, e=Profit, g=Investment, i=Employees.
**Positive labour_cost net balance = costs rising (adverse).**

---

### mart_safe__outlook

Expected turnover (sub_item='a') and investment (sub_item='b') changes over next 2 quarters (Q26).
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.
Net balance = % expecting increase − % expecting decrease.

---

### mart_safe__availability_expectations

Expected availability of external financing (Q23) over the next quarter.
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.
Sub-items: b=Bank loans, d=Trade credit, g=Credit lines and bank overdraft.
`net_balance_wtd` = % expecting improvement − % expecting deterioration.
Positive = more firms expect improvement (FAVOURABLE). Non-response codes 7/9 excluded.

---

### mart_safe__expectations

Q31 (inflation rate expectations), Q33 (inflation risk direction), Q34 (expected % changes
in prices, wages, employment). Includes weighted mean and unweighted percentiles.
`firm_size`: `'all'` = all respondents, `'sme'` = bands 1–3. Wave 30 (2024Q1) onward.

---

### mart_safe__survey_participants

Firm counts and SME/large composition by wave × country × firm_size.
Includes an `'EA'` pseudo-country row summing all euro-area countries.
Use this to understand sample sizes before quoting net balances.

**Key columns**: `n_firms`, `n_firms_wtd`, `pct_sme`, `pct_large`
**Note**: `pct_sme`/`pct_large` are only meaningful on `firm_size = 'all'` rows.

```sql
-- Slovakia vs EA sample sizes, latest wave
SELECT country_code, firm_size, n_firms, n_firms_wtd, pct_sme
FROM main_safe.mart_safe__survey_participants
WHERE wave_number = 38 AND country_code IN ('SK', 'EA') AND firm_size = 'all'
```

---

### mart_safe__question_coverage

Response rates for every question × sub-item by wave × country × firm_size.
Covers all questions (Q2, Q5, Q6A, Q7A/B, Q9, Q10, Q26, Q31, Q33, Q34, Q0B).
Includes an `'EA'` pseudo-country row.

**Key columns**: `n_total`, `n_valid`, `n_nonresponse`, `response_rate_wtd`
**Note**: routed questions (Q5/Q9/Q10) have `n_total` << total participants because
only firms for whom the instrument is relevant are asked.

```sql
-- Coverage for Q5/Q9 bank loans in Slovakia, wave 38
SELECT question_id, sub_item, firm_size, n_total, n_valid, response_rate_wtd
FROM main_safe.mart_safe__question_coverage
WHERE wave_number = 38 AND country_code = 'SK'
  AND question_id IN ('q5', 'q9') AND sub_item = 'a'
ORDER BY question_id, firm_size
```

---

## Methodology Notes

### Coverage
All marts cover **wave 30 (2024Q1) onward**, three-month reference period only.
For historical data (waves 1–29, 2009–2023), query `int_safe__core_questions_long` directly.

### Weights
Use `weight_common` for all aggregations. `weight_enterprise` is not populated in the microdata.

### SME definition
`is_sme = true` ↔ `employee_band_code BETWEEN 1 AND 3` (micro 1–9, small 10–49, medium 50–249).
Large firms = band 4 (250+ employees).

### Firm-size scope
All marts produce rows for both `firm_size = 'all'` (all valid respondents) and `firm_size = 'sme'`
(employee_band_code 1–3). Report SQL queries filter to `firm_size = 'all'` by default.
When SK SME values diverge from SK all-firms by ≥30pp, `run_report.py` appends a divergence note
to the LLM bullet prompt so the AI can surface it. Exception: `mart_safe__slovakia_kpis`
is pre-joined SK SME data only (no `firm_size` column).

### Net balance sign conventions
- Q5 need: positive = more firms report increased need (demand for credit rising)
- Q9 availability: positive = more firms report improved availability (supply easing)
- Q10 terms: positive = bank tightened terms (rates up, collateral up) — **adverse**
- Q0B pressingness: higher score = problem more pressing (not a net balance)
- Q2 business situation: positive = indicator rising; for labour costs, positive = costs rising
