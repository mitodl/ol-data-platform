-- ad_opt_v2's campaign-day report (reports/campaign-day-report.csv).
with courses as (
    select course, account_id, min_date from {{ ref('google_ads_courses') }}
)

select
    courses.course
    , campaign_daily.report_date
    , campaign_daily.campaign_id
    , campaign_daily.campaign_name
    , campaign_daily.impressions
    , campaign_daily.search_impression_share
    , campaign_daily.search_budget_lost_impression_share
    , campaign_daily.search_rank_lost_impression_share
from {{ ref('stg__google_ads__api__campaign_daily') }} as campaign_daily
inner join courses
    on
        campaign_daily.account_id = courses.account_id
        and campaign_daily.report_date >= courses.min_date
where
    {{ google_ads_is_search_campaign('campaign_daily.campaign_name', 'campaign_daily.campaign_channel_type') }}
    and campaign_daily.impressions > 0
