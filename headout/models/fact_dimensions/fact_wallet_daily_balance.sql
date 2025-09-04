{{
    config(
        materialized='incremental',
        strategy='append',
        unique_key = ['cal_date', 'wallet_id']
    )
}}


with dates as (
    select
        *
    from {{ ref('dim_calendar') }}
    where cal_date >= (select min(issue_at::date) from {{ ref('stg_wallet_credit') }})
    and cal_date <= (select max(coalesce(redeemed_at::date, current_date)) from {{ ref('stg_wallet_credits_redeemed') }})
),


credits_daily as (
    select
        issued_at::date as cal_date,
        wallet_id,
        sum(credit_value_usd) as credits_issued_usd
    from {{ ref('stg_wallet_credit') }}
    group by 1,2
),

redemptions_daily as (
    select
        redeemed_at::date as cal_date,
        wallet_id,
        sum(redeemed_value_usd) as redeemed_usd
    from {{ ref('stg_wallet_credits_redeemed') }}
    group by 1,2
),

agg as (
    select
        d.cal_date,
        w.wallet_id,
        sum(coalesce(c.credits_issued_usd,0)) over (
            partition by w.wallet_id order by d.cal_date
            rows between unbounded preceding and current row
        ) as total_credits_issued_usd,
        sum(coalesce(r.redeemed_usd,0)) over (
            partition by w.wallet_id order by d.cal_date
            rows between unbounded preceding and current row
        ) as total_redeemed_usd
    from dates d
    cross join (select distinct wallet_id from {{ ref('stg_wallet_credit') }}) w
    left join credits_daily c
      on c.cal_date = d.cal_date and c.wallet_id = w.wallet_id
    left join redemptions_daily r
      on r.cal_date = d.cal_date and r.wallet_id = w.wallet_id
)


select
    cal_date,
    wallet_id,
    total_credits_issued_usd,
    total_redeemed_usd,
    (total_credits_issued_usd - total_redeemed_usd) as wallet_balance_usd,
from agg   

{% if is_incremental() %}
    where cal_date > (select max(cal_date) from {{ this }})
{% endif %}   