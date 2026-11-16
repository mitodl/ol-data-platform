with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__customer_clients') }}
)

select
    cast(customer_client__id as varchar) as customer_id
    , customer_client__descriptive_name as account_name
    , customer_client__manager as account_is_manager
    , customer_client__status as account_status
    , customer_client__level as account_level
    , customer_client__currency_code as account_currency_code
    , customer_client__time_zone as account_time_zone
    , retrieved_at as account_retrieved_at
from source
