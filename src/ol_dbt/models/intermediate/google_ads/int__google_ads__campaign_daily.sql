-- ad_opt_v2's campaign-day report (reports/campaign-day-report.csv).
-- A campaign no google_ads_campaign_courses row maps keeps its rows, with no course.
with courses as (
    select account_id, campaign_id, course from {{ ref('int__google_ads__campaign_courses') }}
)

, history_starts as (
    select account_id, history_start_date from {{ ref('google_ads_history_accounts') }}
)

-- Every row under its campaign's latest name, as ad_opt_v2's pulls see it.
, current_names as (
    select account_id, campaign_id, latest_name
    from {{ ref('int__google_ads__campaign_names') }}
    where name = latest_name
)

select
    campaign_daily.account_id
    , courses.course
    , campaign_daily.report_date
    , campaign_daily.campaign_id
    , coalesce(current_names.latest_name, trim(campaign_daily.campaign_name)) as campaign_name
    , campaign_daily.impressions
    , campaign_daily.search_impression_share
    , campaign_daily.search_budget_lost_impression_share
    , campaign_daily.search_rank_lost_impression_share
from {{ ref('stg__google_ads__api__campaign_daily') }} as campaign_daily
left join courses
    on campaign_daily.account_id = courses.account_id and campaign_daily.campaign_id = courses.campaign_id
left join history_starts on campaign_daily.account_id = history_starts.account_id
left join current_names
    on campaign_daily.account_id = current_names.account_id and campaign_daily.campaign_id = current_names.campaign_id
where
    {{ google_ads_is_search_campaign('campaign_daily.campaign_name', 'campaign_daily.campaign_channel_type') }}
    and campaign_daily.impressions > 0
    and (history_starts.history_start_date is null or campaign_daily.report_date >= history_starts.history_start_date)
