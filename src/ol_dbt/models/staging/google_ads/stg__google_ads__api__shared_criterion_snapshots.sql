with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__shared_criterion_snapshots') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__shared_criterion_snapshots'
    , 'snapshot_date'
    , ['customer_id', 'snapshot_date', 'shared_criterion__resource_name']
) }}

select
    customer_id as account_id
    , cast(snapshot_date as date) as snapshot_date
    , shared_criterion__resource_name as shared_criterion_resource_name
    , shared_set__id as shared_set_id
    , shared_criterion__type as shared_criterion_type
    , shared_criterion__keyword__text as keyword_text
    , shared_criterion__keyword__match_type as keyword_match_type
    , retrieved_at as shared_criterion_snapshot_retrieved_at
from current_source
