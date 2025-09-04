{{
    config(
        materialized='incremental',
        strategy='insert+delete'
        unique_key='credit_id'
    )
}}


with src as (
    select
        credit_transaction:id::string              as credit_id,
        credit_transaction:wallet_id::string       as wallet_id,
        credit_transaction:type::string            as credit_type,
        credit_transaction:value::number           as credit_value,
        credit_transaction:currency_code::string   as currency_code,
        credit_transaction:value_usd::number       as credit_value_usd,
        to_timestamp_tz(credit_transaction:creation_timestamp::string) as issued_at,
        to_timestamp_tz(credit_transaction:expiry_timestamp::string)   as expiry_at,
        loaded_at::timestamptz                      as loaded_at
    from {{ source('headout_sources', 'wallet_credits_added') }}
    {% if is_incremental() %}
        where loaded_at >= (select max(loaded_at) from {{ this }})
    {% endif %}
)
select * from src