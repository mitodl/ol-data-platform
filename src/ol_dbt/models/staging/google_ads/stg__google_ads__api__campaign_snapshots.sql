with source as (
    select
        campaign_snapshots.*
        , coalesce(customer_clients.customer_client__time_zone, {{ google_ads_default_time_zone() }})
        as account_time_zone
    from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__campaign_snapshots') }} as campaign_snapshots
    left join {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__customer_clients') }} as customer_clients
        on campaign_snapshots.customer_id = cast(customer_clients.customer_client__id as varchar)
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
    , {{ google_ads_local_to_utc('campaign__start_date_time', 'account_time_zone') }}
    as campaign_start_at
    , {{ google_ads_local_to_utc("nullif(campaign__end_date_time, '')", 'account_time_zone') }} as campaign_end_at
    , campaign__bidding_strategy_type as campaign_bidding_strategy_type
    , campaign_budget__resource_name as campaign_budget_resource_name
    , {{ google_ads_micros('campaign_budget__amount_micros') }} as campaign_budget_amount
    , campaign_budget__reference_count as campaign_budget_reference_count
    , retrieved_at as campaign_snapshot_retrieved_at
    , _dlt_load_id as campaign_snapshot_dlt_load_id
from current_source
