{{
    config(
        materialized='incremental',
        strategy='insert+delete'
        unique_key='redemption_id'
    )
}}


with src as (
    select
        md5(concat_ws(
            '-', 
            credit_transaction:wallet_id::string,
            credit_transaction:creation_timestamp::string,
            credit_transaction:value_usd::string
        ))                                                             as redemption_id
        credit_transaction:wallet_id::string                           as wallet_id,
        credit_transaction:value::number                               as redeemed_value,
        credit_transaction:currency_code::string                       as currency_code,
        credit_transaction:value_usd::number                           as redeemed_value_usd,
        to_timestamp_tz(credit_transaction:creation_timestamp::string) as redeemed_at,
        loaded_at
    from {{ source('headout_sources', 'wallet_credits_redeemed') }}

    {% if is_incremental() %}
        where loaded_at >= (select max(loaded_at) from {{ this }})
    {% endif %}

)
select * from src