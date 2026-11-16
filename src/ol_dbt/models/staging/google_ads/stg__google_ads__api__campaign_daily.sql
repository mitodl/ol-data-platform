with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__campaign_daily') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__campaign_daily'
    , 'segments__date'
    , ['customer_id', 'segments__date', 'campaign__id']
) }}

select
    customer_id as account_id
    , cast(segments__date as date) as report_date
    , campaign__id as campaign_id
    , campaign__name as campaign_name
    , campaign__status as campaign_status
    , campaign__advertising_channel_type as campaign_channel_type
    , {{ google_ads_micros('campaign_budget__amount_micros') }} as campaign_budget_amount
    , metrics__impressions as impressions
    , metrics__clicks as clicks
    , {{ google_ads_micros('metrics__cost_micros') }} as cost
    , metrics__search_impression_share as search_impression_share
    , metrics__search_budget_lost_impression_share as search_budget_lost_impression_share
    , metrics__search_rank_lost_impression_share as search_rank_lost_impression_share
    , retrieved_at as campaign_daily_retrieved_at
    , _dlt_load_id as campaign_daily_dlt_load_id
from current_source
