# SAFE Survey — AI Assistant Context

## What is SAFE?

The **Survey on the Access to Finance of Enterprises (SAFE)** is a joint ECB/European Commission survey conducted quarterly since 2009. It covers approximately 5,000 euro area firms per wave, of which ~92% are SMEs (fewer than 250 employees). The survey asks firms about their recent financing experiences, business situation, cost pressures, and forward-looking expectations.

This database contains the full SAFE microdata across all waves (Wave 1 = 2009H1 through Wave 37 = 2025Q4), giving access to a panel of millions of firm-wave observations across the euro area.

---

## Database Structure

| Model | Type | Description |
|-------|------|-------------|
| `stg_safe__microdata` | view | Raw staging layer — all 616 source columns cast to correct types |
| `int_safe__firm_survey_responses` | table | Firm spine with decoded labels (country, size, sector, wave metadata) |
| `int_safe__core_questions_long` | table | Core survey questions Q2–Q34 unpivoted into long format (~26M rows) |
| `int_safe__q0b_pressingness` | table | Q0B "pressing problems" in long format with decoded labels |
| `mart_safe__business_situation` | table | Weighted net balances for Q2 (turnover, costs, profit, employment) |
| `mart_safe__financing_conditions` | table | Weighted net balances for Q9 (availability) and Q10 (bank loan terms) |
| `mart_safe__loan_applications` | table | Application rates, discouragement, rejection rates, financing gap (Q7A/Q7B) |
| `mart_safe__outlook` | table | Expected turnover and fixed investment changes (Q26) |
| `mart_safe__expectations` | table | Inflation expectations (Q31), inflation risk direction (Q33), expected price/wage/employment changes (Q34) |
| `mart_safe__q0b_pressingness` | table | Weighted average pressingness scores by country × wave × problem |

All tables are in the `main_safe` schema of the `my_db` MotherDuck database. Mart tables are pre-aggregated to **wave × country × sub-item** and restricted to **SMEs only** (employee_band_code 1–3: micro, small, medium).

---

## Key Variables and Concepts

### Firm identifiers
- `permid` — anonymised firm identifier, stable across waves
- `wave_number` — survey wave (1 = 2009H1, 37 = 2025Q4)
- `survey_period_label` — human-readable period (e.g. "2024 Q3")

### Firm characteristics
- `employee_band_code` — 1=micro (1–9), 2=small (10–49), 3=medium (50–249), 4=large (250+)
- `sector_code` / `sector_en` — NACE sector (construction, trade, transport, industry, other services)
- `country_code` / `country_name_en` — ISO country code + full name
- `weight_common` — ECB calibration weight; always use for weighted analysis

### Response conventions
- Net balance = % reporting increase/improvement − % reporting decrease/deterioration (range −100 to +100)
- Non-response codes are retained in the intermediate layer and flagged via `is_nonresponse`; mart tables exclude them from aggregations

---

## Survey Questions Covered

| Question | Topic | Response type |
|----------|-------|---------------|
| Q0B | How pressing are business problems? (finding customers, competition, access to finance, costs, skilled staff, regulation) | Scale 1–10 |
| Q2 | Business situation — changes in turnover, costs, profit, employment, debt | 1=increased, 2=unchanged, 3=decreased |
| Q4rec | Financing sources — relevance and recent use (bank loans, trade credit, leasing, equity, etc.) | 1=used, 2=relevant not used, 7=not relevant |
| Q5 | Change in need for external financing by instrument | 1=increased, 2=unchanged, 3=decreased |
| Q6A | Purpose of external financing used | Multi-select: investment, working capital, hiring, new products, refinancing, other |
| Q7A | Application status for bank loans, trade credit, credit lines, other | 1=applied, 2=discouraged, 3=sufficient funds, 4=other reason |
| Q7B | Outcome of financing application | 1=received all, 5=received 75%+, 6=received <75%, 4=rejected, 3=too costly |
| Q9 | Availability of financing types over past 6 months | 1=improved, 2=unchanged, 3=deteriorated |
| Q10 | Bank loan terms and conditions (interest rates, fees, collateral, maturity, loan size) | 1=increased, 2=unchanged, 3=decreased |
| Q11 | Factors affecting financing availability (economy, own capital, credit history, bank willingness) | 1=improved, 2=unchanged, 3=deteriorated |
| Q23 | Expected availability of financing over next 6 months | 1=will improve, 2=unchanged, 3=will deteriorate |
| Q26 | Expected change in turnover and fixed investment over next two quarters | 1=increase, 2=unchanged, 3=decrease |
| Q31 | Expected euro area HICP inflation (in 12 months, 3 years, 5 years) | Continuous %, e.g. 2.5 = 2.5% |
| Q33 | Main risk to 5-year inflation outlook | 1=downside, 2=balanced, 3=upside |
| Q34 | Expected % change in own selling prices, input costs, wages, employment over next 12 months | Continuous % |

---

## Types of Questions You Can Ask

### Financing conditions
- "What has happened to bank loan interest rates in Germany over the past 4 years?"
- "Which euro area countries have the tightest collateral requirements?"
- "How has the availability of credit lines changed since 2022?"
- "Compare the net balance for bank loan availability between small and medium firms."

### Financing gap and access to credit
- "What share of SMEs were discouraged from applying for bank loans in 2024?"
- "Which countries have the highest financing gap? How has it evolved?"
- "What fraction of firms that applied for a bank loan were rejected in Spain?"
- "How does discouragement differ by firm size or sector?"
- "Are firms credit constrained? The analysis should check whether the availability of credit is lower than the need for credit."

### Business situation and cost pressures
- "Have European SMEs' profit margins been declining? Show the net balance for profit over recent waves."
- "Which costs have risen most — labour, energy/materials, or interest expenses?"
- "Compare turnover net balances across euro area countries in the latest wave."
- "How has debt-to-assets changed for SMEs across the cycle?"

### Pressing business problems (Q0B)
- "Which business problem do euro area SMEs rate as most pressing?"
- "Has 'access to finance' become more or less pressing since 2022?"
- "Compare the pressingness of 'costs of production' across countries."
- "Show the trend in 'finding customers' pressingness for Germany vs France."

### Loan applications and outcomes
- "What percentage of SMEs applied for a bank loan in the latest wave?"
- "How has the rejection rate evolved since 2009?"
- "Which instrument has the highest discouragement rate?"
- "Break down loan application outcomes by country for the most recent wave."

### Business outlook and expectations
- "Are SMEs optimistic or pessimistic about turnover growth in the next two quarters?"
- "What are SMEs expecting for wage growth in the next 12 months?"
- "How do SME inflation expectations compare to the ECB's 2% target?"
- "Which countries show the most divergent investment expectations?"

### Sector and size comparisons
- "Do construction firms report worse financing conditions than trade firms?"
- "Are micro firms more discouraged from borrowing than small firms?"
- "Compare SME vs large firm assessments of bank lending conditions."

---

## Important Notes for Analysis

1. **Always use `weight_common` for representative estimates.** Raw counts are informative but not population-representative without the ECB calibration weight.

2. **Net balance interpretation**: Positive = more firms improved/increased than deteriorated/decreased. A net balance of +10 on bank loan availability means 10 percentage points more firms saw improvement than deterioration.

3. **3-month questionnaire**: From Wave 30 (2024H1) some waves use a quarterly reference period. These responses are in `response_3m` columns. The mart models use `coalesce(response_raw, response_3m)` to handle both.

4. **Non-response codes**: In the intermediate layer, non-responses are retained with `is_nonresponse = true`. All mart aggregations exclude non-responses from numerators and denominators.

5. **Q4rec code 7** means "not relevant" (a substantive answer, not a non-response). Only codes 9 and 99 are non-response for Q4rec.

6. **Scope of mart tables**: All mart tables are pre-filtered to SMEs (employee_band_code 1–3) for comparability with ECB published aggregates. For large firms, query the intermediate tables directly.

7. **Wave numbering**: Wave 1 = 2009 H1 (first half). From Wave 30 (2024H1) the survey moved to quarterly frequency.
