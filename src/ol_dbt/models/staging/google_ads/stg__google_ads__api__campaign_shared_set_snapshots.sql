with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__campaign_shared_set_snapshots') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__campaign_shared_set_snapshots'
    , 'snapshot_date'
    , ['customer_id', 'snapshot_date', 'campaign_shared_set__resource_name']
) }}

select
    customer_id
    , cast(snapshot_date as date) as snapshot_date
    , campaign_shared_set__resource_name as campaign_shared_set_resource_name
    , campaign__id as campaign_id
    , shared_set__id as shared_set_id
    , shared_set__name as shared_set_name
    , shared_set__type as shared_set_type
    , campaign_shared_set__status as campaign_shared_set_status
    , retrieved_at
from current_source
