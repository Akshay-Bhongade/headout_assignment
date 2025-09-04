## Assumtions
1. **Snowflake** is used as the data warehouse.  

2. Raw data is ingested as JSON payloads, as described in the assignment.  

   Two source tables are assumed:  

   a. **wallet_credits_added**  
   ```sql
   CREATE TABLE wallet_credits_added (
       credit_transaction VARIANT,
       loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
   );
