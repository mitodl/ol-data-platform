-- ad_opt_v2's generate_campaign_day_panel: each Search campaign's clicks, cost and conversions per
-- day under the version that held that day, with zero rows for serving days that had no cost.
with keyword_days as (
    select course, report_date, campaign_name, campaign_region, clicks, cost, all_conversions
    from {{ ref('int__google_ads__keyword_daily') }}
    where course is not null
)

, versions as (
    select
        course
        , campaign_version
        , campaign_name
        , campaign_region
        , start_date
        , end_date
        , campaign_serving_status
        , daily_budget
        , match_types
    from {{ ref('int__google_ads__campaign_versions') }}
)

, campaign_list as (
    select course, campaign_name from keyword_days
    union
    select course, campaign_name from versions
)

, excluded as (
    {{ google_ads_excluded_campaigns('campaign_list') }}
)

, included_days as (
    select keyword_days.*
    from keyword_days
    left join excluded
        on keyword_days.course = excluded.course and keyword_days.campaign_name = excluded.campaign_name
    where excluded.campaign_name is null
)

, included_versions as (
    select versions.*
    from versions
    left join excluded
        on versions.course = excluded.course and versions.campaign_name = excluded.campaign_name
    where excluded.campaign_name is null
)

, last_days as (
    select course, max(report_date) as last_day
    from included_days
    group by course
)

-- Days the campaign spent nothing are dropped, as the panel keeps only days with cost.
, campaign_day_totals as (
    select
        course
        , report_date
        , campaign_name
        , campaign_region
        , sum(clicks) as clicks
        , sum(cost) as cost
        , sum(all_conversions) as all_conversions
    from included_days
    group by course, report_date, campaign_name, campaign_region
    having sum(cost) > 0
)

, version_days as (
    select
        campaign_day_totals.course
        , campaign_day_totals.report_date
        , included_versions.campaign_version
        , campaign_day_totals.campaign_name
        , campaign_day_totals.campaign_region
        , included_versions.daily_budget
        , included_versions.match_types
        , campaign_day_totals.clicks
        , campaign_day_totals.cost
        , campaign_day_totals.all_conversions
    from campaign_day_totals
    inner join included_versions
        on
            campaign_day_totals.course = included_versions.course
            and campaign_day_totals.campaign_name = included_versions.campaign_name
            and campaign_day_totals.report_date >= included_versions.start_date
            and (included_versions.end_date is null or campaign_day_totals.report_date < included_versions.end_date)
)

-- Every day of an active version, up to its end or the panel's last day.
, active_version_days as (
    select
        bounded.course
        , cast({{ dbt.dateadd('day', 'days.day_number - 1', 'bounded.start_date') }} as date) as report_date
        , bounded.campaign_version
        , bounded.campaign_name
        , bounded.campaign_region
        , bounded.daily_budget
        , bounded.match_types
        , bounded.end_before
    from (
        select
            included_versions.*
            , coalesce(
                included_versions.end_date
                , cast({{ dbt.dateadd('day', 1, 'last_days.last_day') }} as date)
            ) as end_before
        from included_versions
        inner join last_days on included_versions.course = last_days.course
        where included_versions.campaign_serving_status = 'active'
    ) as bounded
    cross join
        {{ unnest_sequence(
            'greatest(' ~ date_diff("'day'", 'bounded.start_date', 'bounded.end_before') ~ ', 1)', 'days', 'day_number'
        ) }}
)

, zero_days as (
    select
        active_version_days.course
        , active_version_days.report_date
        , active_version_days.campaign_version
        , active_version_days.campaign_name
        , active_version_days.campaign_region
        , active_version_days.daily_budget
        , active_version_days.match_types
        , 0 as clicks
        , 0 as cost
        , 0 as all_conversions
    from active_version_days
    left join version_days
        on
            active_version_days.course = version_days.course
            and active_version_days.campaign_version = version_days.campaign_version
            and active_version_days.report_date = version_days.report_date
    where active_version_days.report_date < active_version_days.end_before and version_days.course is null
)

, panel_days as (
    select * from version_days
    union all
    select * from zero_days
)

-- Impressions sum and shares average when a name covers two campaigns on a day.
, impression_days as (
    select
        course
        , report_date
        , campaign_name
        , sum(impressions) as impressions
        , avg(search_impression_share) as search_impression_share
        , avg(search_budget_lost_impression_share) as search_budget_lost_impression_share
        , avg(search_rank_lost_impression_share) as search_rank_lost_impression_share
    from {{ ref('int__google_ads__campaign_daily') }}
    group by course, report_date, campaign_name
)

select
    panel_days.course
    , panel_days.report_date
    , panel_days.campaign_version
    , panel_days.campaign_name
    , panel_days.campaign_region
    , panel_days.daily_budget
    , panel_days.match_types
    , panel_days.clicks
    , round(panel_days.cost, 2) as cost
    , round(panel_days.all_conversions, 4) as all_conversions
    , coalesce(impression_days.impressions, 0) as impressions
    , impression_days.search_impression_share
    , impression_days.search_budget_lost_impression_share
    , impression_days.search_rank_lost_impression_share
from panel_days
left join impression_days
    on
        panel_days.course = impression_days.course
        and panel_days.report_date = impression_days.report_date
        and panel_days.campaign_name = impression_days.campaign_name
