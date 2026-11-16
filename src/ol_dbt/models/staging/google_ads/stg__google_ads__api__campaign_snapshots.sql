with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__campaign_snapshots') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__campaign_snapshots'
    , 'snapshot_date'
    , ['customer_id', 'snapshot_date', 'campaign__resource_name']
) }}

select
    customer_id as account_id
    , cast(snapshot_date as date) as snapshot_date
    , campaign__resource_name as campaign_resource_name
    , campaign__id as campaign_id
    , campaign__name as campaign_name
    , campaign__status as campaign_status
    , campaign__advertising_channel_type as campaign_channel_type
    , campaign__start_date_time as campaign_start_at
    , {{ cast_to_timestamptz("cast(nullif(campaign__end_date_time, '') as timestamp)") }} as campaign_end_at
    , campaign__bidding_strategy_type as campaign_bidding_strategy_type
    , campaign_budget__resource_name as campaign_budget_resource_name
    , {{ google_ads_micros('campaign_budget__amount_micros') }} as campaign_budget_amount
    , campaign_budget__reference_count as campaign_budget_reference_count
    , retrieved_at as campaign_snapshot_retrieved_at
    , _dlt_load_id as campaign_snapshot_dlt_load_id
from current_source
