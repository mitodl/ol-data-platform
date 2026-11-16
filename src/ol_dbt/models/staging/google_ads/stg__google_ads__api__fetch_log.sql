with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__fetch_log') }}
)

select
    customer_id as account_id
    , table_name as fetch_table_name
    , cast(window_start as date) as fetch_window_start
    , cast(window_end as date) as fetch_window_end
    , retrieved_at as fetch_retrieved_at
    , _dlt_load_id as fetch_dlt_load_id
from source
