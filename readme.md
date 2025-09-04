## Assumtions
    1. using snowflake as warehousing solution. 

    2. raw data is populated as json payloads as discribed in the assignment:

        Assumed two sources here:
        ** wallet_credit_added **
        DDL SQL:
        create table wallet_credits_added(
        credit_transaction variant,
        loaded_at timestamp_ntz default current_timestamp
        );

        **wallet_credit_redeemed**
        DDL SQL:
        create table wallet_credits_redeemed (
        redeem_transaction variant,
        loaded_at timestamp default current_timestamp
        );
