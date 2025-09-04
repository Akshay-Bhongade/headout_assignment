## Assumtions
1. **Snowflake** is used as the data warehouse. 


2. Raw data is ingested as JSON payloads, as described in the assignment.  

   Two source tables are assumed:

    a. **wallet_credits_added**  
    ```sql
    CREATE TABLE wallet_credits_added (
        credit_transaction VARIANT,
        loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    )
    ```

    b. **wallet_credits_redeemed**
    ```sql
    CREATE TABLE wallet_credits_added (
        credit_transaction VARIANT,
        loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
    );
    ```
    Default timestamp here is used for loading incrementally in the staging and downstream layers.


    Defined in source.yml:
    ![source.yml](image.png)



3. Since its not specified how a wallet is redeemed, we assume here that the ***credits that are expiring first are consumed first***.

    This assumtion helps simplify ***"redemption allocation"** and help maintain credits usage (fully/ partially consumed) accurately.




## ER Diagram
```mermaid
erDiagram

    %% Raw Event Tables
    WALLET_CREDITS_ADDED {
        variant payload
        timestamp loaded_at
    }

    WALLET_CREDITS_REDEEMED {
        variant payload
        timestamp loaded_at
    }

    %% Staging
    STG_WALLET_CREDIT {
        string credit_id PK
        string wallet_id
        string credit_type
        string currency_code
        number credit_value_usd
        timestamp issued_at
        timestamp expiry_at
        timestamp loaded_at
    }

    STG_WALLET_REDEEMED {
        string redemption_id PK
        string wallet_id
        number redeemed_value_usd
        timestamp redeemed_at
        timestamp loaded_at
    }

    %% Dimension
    DIM_CALENDAR {
        date cal_date PK
        int cal_year
        int cal_month
    }

    %% Facts
    FACT_WALLET_CREDITS {
        string credit_id PK
        string wallet_id FK
        string credit_type
        string currency_code
        number credit_value_usd
        number redeemed_value_usd
        number remaining_value_usd
        string status
        timestamp issued_at
        timestamp expiry_at
    }

    FACT_WALLET_REDEMPTION_ALLOCATION {
        string redemption_id PK
        string credit_id PK
        string wallet_id FK
        number redeemed_value_usd
        timestamp redeemed_at
    }

    FACT_WALLET_DAILY_AGG {
        string wallet_id PK
        date cal_date PK
        number total_credits_issued_usd
        number total_redeemed_usd
        number balance_usd
    }

    %% Macro as processing node (pseudo-entity)
    APPLY_REDEMPTIONS_ROW_BY_ROW {
        dbt_macro process
    }

    %% Relationships
    WALLET_CREDITS_ADDED ||--|| STG_WALLET_CREDIT : "flattened_in"
    WALLET_CREDITS_REDEEMED ||--|| STG_WALLET_REDEEMED : "flattened_in"

    STG_WALLET_CREDIT ||--|| FACT_WALLET_CREDITS : "feeds only new credit transaction"
    STG_WALLET_REDEEMED ||--o{ FACT_WALLET_REDEMPTION_ALLOCATION : "uses macro to insert allocations"

    STG_WALLET_REDEEMED ||--|| APPLY_REDEMPTIONS_ROW_BY_ROW : "consumes recent redemption"
    FACT_WALLET_CREDITS ||--|| APPLY_REDEMPTIONS_ROW_BY_ROW : " consumes latest credit state"

    APPLY_REDEMPTIONS_ROW_BY_ROW ||--o{ FACT_WALLET_REDEMPTION_ALLOCATION : "produces"
    APPLY_REDEMPTIONS_ROW_BY_ROW ||--o{ FACT_WALLET_CREDITS : "updates"

    DIM_CALENDAR ||--|| FACT_WALLET_DAILY_AGG : "date spline"

    STG_WALLET_CREDIT ||--o{ FACT_WALLET_DAILY_AGG : "summarized_in"
    STG_WALLET_REDEEMED ||--o{ FACT_WALLET_DAILY_AGG : "summarized_in"

   
```

## Business Questions

1. What is the current total balance of all credits in the Headout wallet service?

    Ans:
    After the macro "***redemption_allocation_macro***" is ran, ***fact_wallet_credit_balance*** represents the latest state for each credit transaction after redemptions are applied.
    Summing on the **remaining_value_usd** gives us the current balance in credits.

    ```sql

    select 
        sum(remaining_value_usd) as balance
    from {{ref(fact_wallet_credit_balance)}}

    ```

2. What is the balance of each credit type (cancellation, goodwill, gift_card)?

    Ans:
    Extending on above, we can add more granularity to the query to get the desired result.
    Sum over **credit_type** will gives the current balance on each type.

    ```sql

    select 
        distinct credit_type,
        sum(remaining_value_usd) as balance
    from {{ref(fact_wallet_credit_balance)}}

    ```

3. What is the daily total wallet balance over time?

    Ans:
    [***fact_wallet_daily_agg***](headout/models/fact_dimensions/fact_wallet_daily_agg.sql) aggregates rolling sum of **credits added** and **redeemed** to each wallet. 
    Ultimately giving the change in balance for each wallet every day.

     ```sql

    select
        cal_date,
        wallet_id,
        total_credits_issued_usd,
        total_redeemed_usd,
        wallet_balance_usd,
    from {{ref('fact_wallet_daily_agg')}}
    where wallet_id = "xyz"

    ```