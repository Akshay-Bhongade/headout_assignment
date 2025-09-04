{{ config(
    materialized='table'
) }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2027-12-31' as date)"
    ) }}

),

calendar as (

    select
        date_day as cal_date,
        extract(year from date_day) as cal_year,
        extract(month from date_day) as cal_month,
    from date_spine

)

select * from calendar
order by cal_date