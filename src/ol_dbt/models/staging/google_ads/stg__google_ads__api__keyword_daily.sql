with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__keyword_daily') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__keyword_daily'
    , 'segments__date'
    , ['customer_id', 'segments__date', 'segments__ad_network_type', 'ad_group__id', 'ad_group_criterion__criterion_id']
) }}

select
    customer_id as account_id
    , cast(segments__date as date) as report_date
    , segments__ad_network_type as ad_network_type
    , campaign__id as campaign_id
    , campaign__name as campaign_name
    , campaign__status as campaign_status
    , campaign__advertising_channel_type as campaign_channel_type
    , ad_group__id as ad_group_id
    , ad_group__name as ad_group_name
    , ad_group_criterion__criterion_id as keyword_criterion_id
    , ad_group_criterion__keyword__text as keyword_text
    , ad_group_criterion__keyword__match_type as keyword_match_type
    , customer__currency_code as currency_code
    , {{ google_ads_micros('ad_group_criterion__position_estimates__first_page_cpc_micros') }}
    as keyword_first_page_cpc
    , metrics__impressions as impressions
    , metrics__clicks as clicks
    , {{ google_ads_micros('metrics__cost_micros') }} as cost
    , metrics__search_impression_share as search_impression_share
    , retrieved_at as keyword_daily_retrieved_at
    , _dlt_load_id as keyword_daily_dlt_load_id
from current_source
