with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__campaign_criterion_snapshots') }}
)

{{ google_ads_current_rows(
    'raw__google_ads__api__campaign_criterion_snapshots'
    , 'snapshot_date'
    , ['customer_id', 'snapshot_date', 'campaign_criterion__resource_name']
) }}

select
    customer_id as account_id
    , cast(snapshot_date as date) as snapshot_date
    , campaign_criterion__resource_name as campaign_criterion_resource_name
    , campaign__id as campaign_id
    , campaign_criterion__type as campaign_criterion_type
    , campaign_criterion__negative as campaign_criterion_is_negative
    , campaign_criterion__keyword__text as keyword_text
    , campaign_criterion__keyword__match_type as keyword_match_type
    , campaign_criterion__location__geo_target_constant as location_geo_target_constant
    , retrieved_at as campaign_criterion_snapshot_retrieved_at
from current_source
