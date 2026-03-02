{{ config(materialized='table') }}

with spine as (
  select * from {{ ref('int_safe__firm_survey_responses') }}
),
responses as (
  select * from {{ ref('int_safe__core_questions_long') }}
  where questionid in ('q4', 'q5')  -- Availability (Q4), Needs (Q5)
    and responsegrouped is not null  -- Use grouped for net calc; ignore non-resp
),
metrics as (
  select
    -- Segments from spine
    country,
    firmsize,
    issme,
    surveyperiodlabel,
    surveyyear,
    
    -- Metric name
    case 
      when questionid = 'q4' and subitem = 'a' then 'bank_loans_availability'
      when questionid = 'q5' and subitem = 'a' then 'bank_loans_needs'
      -- Add more: 'q10a' interest rates, etc.
    end as metric,
    
    -- Net %: (% increased=3+ - % decreased=1-) / total * 100
    case responsegrouped
      when 3 then 1   -- Increased
      when 1 then -1  -- Decreased
      else 0
    end as net_contrib,
    
    weightcommon  -- For weighted nets if needed
  from spine s
  left join responses r on s.permid = r.permid and s.wavenumber = r.wavenumber
  where metric is not null
)

select 
  country,
  firmsize,
  issme,
  surveyperiodlabel,
  surveyyear,
  metric,
  count(*) as n_firms,
  avg(net_contrib) * 100 as net_percentage,  -- Core ECB-style net %
  sum(case when net_contrib = 1 then 1.0 else 0 end) / count(*) * 100 as pct_increased,
  sum(case when net_contrib = -1 then 1.0 else 0 end) / count(*) * 100 as pct_decreased
from metrics
group by 1,2,3,4,5,6
