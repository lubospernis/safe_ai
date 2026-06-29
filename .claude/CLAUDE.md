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
- All mart tables are pre-aggregated to **wave × country** (and sub-item/question where applicable)
- dbt profile: `safe_ai`, target `prod`

---

## Mart Catalogue

### Which mart to use

| Goal | Mart |
|---|---|
| Financing needs/availability/terms (ECB methodology, all firms) | `mart_safe__financing_conditions` |
| Financing purpose breakdown by country (Q6A) | `mart_safe__financing_purpose` |
| Headline Slovakia KPIs (pre-selected, AI-ready) | `mart_safe__slovakia_kpis` |
| Pressing business problems (Q0B pressingness scores) | `mart_safe__q0b_pressingness` |
| Loan application rates, discouragement, rejection | `mart_safe__loan_applications` |
| Business situation (turnover, profit, labour costs, etc.) | `mart_safe__business_situation` |
| Expected changes in turnover and investment (Q26) | `mart_safe__outlook` |
| Inflation expectations and risk direction (Q31/Q33/Q34) | `mart_safe__expectations` |

---

### mart_safe__financing_conditions

ECB-methodology net balances for Q5 (financing need), Q9 (availability), Q10 (bank loan terms).

**Key rules**:
- `firm_size`: `'all'` = ECB-comparable (all firms), `'sme'` = bands 1–3, `'large'` = band 4
- Reference periods **separate**: `reference_period = '6m'` (ECB white panel) or `'3m'` (grey panel)
- Waves 1–29: only `'6m'` rows. Waves 30+: may have both. Use `'3m'` to match ECB grey panel.
- Non-response codes 7 (N/A) and 9 (DK) excluded from denominator
- Rounded to 1 dp; ECB publishes integers — ±1pp residual expected due to weight precision
- `financing_gap_wtd` on Q9 rows = Q5.net_balance − Q9.net_balance (pre-computed; do not re-derive)
- Q10 net_balance positive = bank **tightened** terms (adverse for firms)

**Instruments**: a, b, c, d, f, g, h (Q5/Q9); a–f (Q10)

```sql
-- ECB Table 1 equivalent for Slovakia Q1 2026 (3m grey panel)
SELECT question_id, sub_item_label, net_balance_wtd
FROM main_safe.mart_safe__financing_conditions
WHERE country_code = 'SK' AND wave_number = 38 AND reference_period = '3m'
  AND question_id IN ('q5', 'q9') AND sub_item IN ('a', 'f')
  AND firm_size = 'all'
ORDER BY question_id, sub_item
```

---

### mart_safe__slovakia_kpis

One row per wave, all headline KPIs for Slovakia SMEs pre-joined and named.
**Always use this for Slovakia trend analysis** — do not recompute from raw marts.

Columns include: `q5a_need_nb`, `q9a_avail_nb`, `bank_loan_gap`, `q10a_interest_nb`,
`turnover_nb`, `profit_nb`, `labour_cost_nb`, `employees_nb`, `investment_nb`,
`press_*` (7 pressingness scores), `bank_loan_app_rate`, `bank_loan_disc_rate`,
`bank_loan_rej_rate`, `bank_loan_access_gap`, `turnover_outlook_nb`, `investment_outlook_nb`.

```sql
SELECT * FROM main_safe.mart_safe__slovakia_kpis ORDER BY wave_number DESC LIMIT 5
```

---

### mart_safe__q0b_pressingness

Weighted average pressingness scores (scale 1–10) for 7 business problems by country × wave.

**Key rules**:
- `reference_period` can be `'6m'` or `'3m'`; prefer `'6m'`, fall back to `'3m'`
- `avg_pressingness_wtd` is a **score 1–10**, NOT a net balance or percentage
- problem_id: 1=Finding customers, 2=Competition, 3=Access to finance,
  4=Costs of production/labour, 5=Skilled staff, 6=Regulation, 7=Other

```sql
SELECT problem_label, avg_pressingness_wtd
FROM main_safe.mart_safe__q0b_pressingness
WHERE country_code = 'SK' AND wave_number = 38 AND reference_period = '6m'
ORDER BY avg_pressingness_wtd DESC
```

---

### mart_safe__financing_purpose

Weighted % of firms citing each purpose for financing (Q6A multi-select). All firms, 6m/3m separate.

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
  AND wave_number = 38 AND reference_period = '6m' AND firm_size = 'sme'
ORDER BY country_code, purpose_id
```

---

### mart_safe__loan_applications

Application rates, discouragement, rejection rates from Q7A/Q7B. SMEs only.

Key columns: `application_rate_wtd`, `discouragement_rate_wtd`, `rejection_rate_wtd`,
`financing_gap_wtd` (= discouragement + rejection share of all respondents — ECB headline access indicator).

`sub_item = 'a'` = bank loans (main instrument of interest).

---

### mart_safe__business_situation

Net balances for Q2 (business situation indicators). SMEs only.
sub_items: a=Turnover, b=Labour costs, e=Profit, g=Investment, i=Employees.
**Positive labour_cost net balance = costs rising (adverse).**

---

### mart_safe__outlook

Expected turnover (sub_item='a') and investment (sub_item='b') changes over next 2 quarters (Q26).
Net balance = % expecting increase − % expecting decrease. SMEs only.

---

### mart_safe__expectations

Q31 (inflation rate expectations), Q33 (inflation risk direction), Q34 (expected % changes
in prices, wages, employment). Includes weighted mean and unweighted percentiles. SMEs only.

---

## Critical Methodology Notes

### Reference period (waves 30+)
From wave 30 (2024Q1) onward the survey moved to quarterly. Two questionnaire versions:
- **6m questionnaire** (response_raw): asked in Q1 and Q3 waves alongside the 3m version
- **3m questionnaire** (response_3m): asked in Q2 and Q4 waves; also alongside 6m in Q1/Q3

ECB publishes white-panel (6m) and grey-panel (3m) figures separately.
`mart_safe__ecb_net_balances` preserves this split via `reference_period`.
`mart_safe__financing_conditions` coalesces both into one figure (not ECB-comparable).

### Weights
Use `weight_common` for all aggregations. `weight_enterprise` is not populated in the microdata.

### SME definition
`is_sme = true` ↔ `employee_band_code BETWEEN 1 AND 3` (micro 1–9, small 10–49, medium 50–249).
Large firms = band 4 (250+ employees).

### Net balance sign conventions
- Q5 need: positive = more firms report increased need (demand for credit rising)
- Q9 availability: positive = more firms report improved availability (supply easing)
- Q10 terms: positive = bank tightened terms (rates up, collateral up) — **adverse**
- Q0B pressingness: higher score = problem more pressing (not a net balance)
- Q2 business situation: positive = indicator rising; for labour costs, positive = costs rising
