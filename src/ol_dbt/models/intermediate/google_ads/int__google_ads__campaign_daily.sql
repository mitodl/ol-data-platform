-- ad_opt_v2's campaign-day report (reports/campaign-day-report.csv).
with products as (
    select product_readable_id, account_id, campaign_id, min_date from {{ ref('int__google_ads__campaign_products') }}
)

-- Every row under its campaign's latest name, as ad_opt_v2's pulls see it.
, current_names as (
    select account_id, campaign_id, latest_name
    from {{ ref('int__google_ads__campaign_names') }}
    where name = latest_name
)

select
    products.product_readable_id
    , campaign_daily.report_date
    , campaign_daily.campaign_id
    , coalesce(current_names.latest_name, trim(campaign_daily.campaign_name)) as campaign_name
    , campaign_daily.impressions
    , campaign_daily.search_impression_share
    , campaign_daily.search_budget_lost_impression_share
    , campaign_daily.search_rank_lost_impression_share
from {{ ref('stg__google_ads__api__campaign_daily') }} as campaign_daily
inner join products
    on
        campaign_daily.account_id = products.account_id
        and campaign_daily.campaign_id = products.campaign_id
        and campaign_daily.report_date >= products.min_date
left join current_names
    on campaign_daily.account_id = current_names.account_id and campaign_daily.campaign_id = current_names.campaign_id
where
    {{ google_ads_is_search_campaign('campaign_daily.campaign_name', 'campaign_daily.campaign_channel_type') }}
    and campaign_daily.impressions > 0
