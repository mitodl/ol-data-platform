with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__keyword_snapshots') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__keyword_snapshots'
    , 'snapshot_date'
    , ['customer_id', 'snapshot_date', 'ad_group_criterion__resource_name']
) }}

select
    customer_id
    , cast(snapshot_date as date) as snapshot_date
    , ad_group_criterion__resource_name as keyword_resource_name
    , campaign__id as campaign_id
    , ad_group__id as ad_group_id
    , ad_group__name as ad_group_name
    , ad_group__status as ad_group_status
    , ad_group_criterion__criterion_id as keyword_criterion_id
    , ad_group_criterion__keyword__text as keyword_text
    , ad_group_criterion__keyword__match_type as keyword_match_type
    , ad_group_criterion__status as keyword_status
    , ad_group_criterion__negative as keyword_is_negative
    , retrieved_at
from current_source
