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


## ER Diagram
```mermaid
erDiagram
    CUSTOMER {
        id INT PK
        email VARCHAR
        password VARCHAR
    }

    BUYER {
        id INT PK
    }

    BUSINESS_CLIENT {
        id INT PK
    }

    ADMINISTRATOR {
        id INT PK
    }

    PRODUCT {
        id INT PK
    }

    CART {
        id INT PK
        customer_id INT FK
    }

    ORDER {
        id INT PK
        customer_id INT FK
    }

    REVIEW {
        id INT PK
        buyer_id INT FK
        product_id INT FK
    }

    WISHLIST {
        id INT PK
        buyer_id INT FK
        product_id INT FK
    }

    SUPPORT_TICKET {
        id INT PK
        customer_id INT FK
    }

    USER_ACCOUNT {
        id INT PK
    }

    PRODUCT_CATALOG {
        id INT PK
    }

    PRODUCT_CATEGORY {
        id INT PK
    }

    SPECIFICATION {
        id INT PK
        product_id INT FK
    }

    ORDER_SHIPMENT {
        id INT PK
        business_client_id INT FK
    }

    DELIVERY_ADDRESS {
        id INT PK
        business_client_id INT FK
    }

    INVOICE {
        id INT PK
        business_client_id INT FK
    }

    ANALYTICS {
        id INT PK
        business_client_id INT FK
    }

    WEBSITE_TRAFFIC {
        id INT PK
    }

    USER_ACTIVITY {
        id INT PK
    }

    SALES_REPORT {
        id INT PK
    }

    CUSTOMER ||--o{ BUYER
    CUSTOMER ||--o{ BUSINESS_CLIENT
    CUSTOMER ||--o{ ADMINISTRATOR
    CUSTOMER ||--|| CART
    CUSTOMER ||--|| ORDER
    CUSTOMER ||--|| SUPPORT_TICKET
    CUSTOMER ||--|| USER_ACCOUNT

    BUYER }o--o{ REVIEW
    BUYER }o--o{ WISHLIST

    BUSINESS_CLIENT ||--|| ORDER_SHIPMENT
    BUSINESS_CLIENT ||--|| DELIVERY_ADDRESS
    BUSINESS_CLIENT ||--|| INVOICE
    BUSINESS_CLIENT ||--|| ANALYTICS

    PRODUCT_CATALOG ||--|| PRODUCT
    PRODUCT_CATEGORY ||--|| PRODUCT
    PRODUCT ||--|| SPECIFICATION

    ADMINISTRATOR ||--|| PRODUCT_CATALOG
    ADMINISTRATOR ||--|| PRODUCT_CATEGORY
    ADMINISTRATOR ||--|| SPECIFICATION
    ADMINISTRATOR ||--|| ORDER
    ADMINISTRATOR ||--|| SUPPORT_TICKET
    ADMINISTRATOR ||--|| USER_ACCOUNT
    ADMINISTRATOR ||--|| WEBSITE_TRAFFIC
    ADMINISTRATOR ||--|| USER_ACTIVITY
    ADMINISTRATOR ||--|| SALES_REPORT

    CUSTOMER }o--o{ DELIVERY_ADDRESS : uses

    

