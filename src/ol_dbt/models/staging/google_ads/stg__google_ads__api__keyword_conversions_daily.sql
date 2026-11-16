with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__keyword_conversions_daily') }}
)

{{ google_ads_current_rows('raw__google_ads__api__keyword_conversions_daily', 'segments__date') }}

select
    customer_id
    , cast(segments__date as date) as report_date
    , segments__ad_network_type as ad_network_type
    , segments__conversion_action as conversion_action_resource_name
    , segments__conversion_action_name as conversion_action_name
    , campaign__id as campaign_id
    , campaign__name as campaign_name
    , campaign__status as campaign_status
    , campaign__advertising_channel_type as campaign_channel_type
    , ad_group__id as ad_group_id
    , ad_group__name as ad_group_name
    , ad_group_criterion__criterion_id as keyword_criterion_id
    , ad_group_criterion__keyword__text as keyword_text
    , ad_group_criterion__keyword__match_type as keyword_match_type
    , metrics__conversions as conversions
    , metrics__all_conversions as all_conversions
    , retrieved_at
from current_source
