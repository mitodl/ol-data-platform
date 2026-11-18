-- ad_opt_v2's keyword-day pull (reports/kw-day-panel.csv), before clean_kw_day_panel,
-- which ads-optimizer runs.
with courses as (
    select
        google_ads_courses.course
        , google_ads_courses.account_id
        , google_ads_courses.min_date
        , networks.setting_value as ad_network
    from {{ ref('google_ads_courses') }} as google_ads_courses
    left join {{ ref('google_ads_course_settings') }} as networks
        on google_ads_courses.course = networks.course and networks.setting = 'ad_network'
)

, conversion_actions as (
    select course, setting_value as conversion_action_name
    from {{ ref('google_ads_course_settings') }}
    where setting = 'conversion_action'
)

-- The pull's row: one ad group keyword per day, over the course's networks, with clicks.
, keyword_rows as (
    select
        courses.course
        , keyword_daily.report_date
        , keyword_daily.campaign_name
        , keyword_daily.ad_group_id
        , keyword_daily.keyword_criterion_id
        , keyword_daily.keyword_text
        , keyword_daily.keyword_match_type
        , sum(keyword_daily.clicks) as clicks
        , sum(keyword_daily.cost) as cost
        , max(keyword_daily.currency_code) as currency_code
        , max(keyword_daily.keyword_first_page_cpc) as keyword_first_page_cpc
    from {{ ref('stg__google_ads__api__keyword_daily') }} as keyword_daily
    inner join courses
        on
            keyword_daily.account_id = courses.account_id
            and keyword_daily.report_date >= courses.min_date
    where
        {{ google_ads_is_search_campaign('keyword_daily.campaign_name', 'keyword_daily.campaign_channel_type') }}
        and keyword_daily.keyword_match_type in ('EXACT', 'PHRASE', 'BROAD')
        and (courses.ad_network is null or keyword_daily.ad_network_type = courses.ad_network)
    group by
        courses.course
        , keyword_daily.report_date
        , keyword_daily.campaign_name
        , keyword_daily.ad_group_id
        , keyword_daily.keyword_criterion_id
        , keyword_daily.keyword_text
        , keyword_daily.keyword_match_type
    having sum(keyword_daily.clicks) > 0
)

-- ad_opt_v2 keys the pull on date, keyword, campaign and match type, and keeps the last
-- ad group's row; this adds the ad groups up, since that order is arbitrary.
, keyword_totals as (
    select
        course
        , report_date
        , campaign_name
        , keyword_text
        , keyword_match_type
        , sum(clicks) as clicks
        , sum(cost) as cost
        , max(currency_code) as currency_code
        , max(keyword_first_page_cpc) as keyword_first_page_cpc
    from keyword_rows
    group by course, report_date, campaign_name, keyword_text, keyword_match_type
)

, conversion_totals as (
    select
        courses.course
        , conversions.report_date
        , conversions.campaign_name
        , conversions.keyword_text
        , conversions.keyword_match_type
        , sum(conversions.all_conversions) as all_conversions
    from {{ ref('stg__google_ads__api__keyword_conversions_daily') }} as conversions
    inner join courses
        on
            conversions.account_id = courses.account_id
            and conversions.report_date >= courses.min_date
    inner join conversion_actions
        on
            courses.course = conversion_actions.course
            and conversions.conversion_action_name = conversion_actions.conversion_action_name
    where
        {{ google_ads_is_search_campaign('conversions.campaign_name', 'conversions.campaign_channel_type') }}
        and conversions.keyword_match_type in ('EXACT', 'PHRASE', 'BROAD')
        and conversions.all_conversions > 0
        and (courses.ad_network is null or conversions.ad_network_type = courses.ad_network)
    group by
        courses.course
        , conversions.report_date
        , conversions.campaign_name
        , conversions.keyword_text
        , conversions.keyword_match_type
)

select
    coalesce(keyword_totals.course, conversion_totals.course) as course
    , coalesce(keyword_totals.report_date, conversion_totals.report_date) as report_date
    , coalesce(keyword_totals.campaign_name, conversion_totals.campaign_name) as campaign_name
    , coalesce(keyword_totals.keyword_text, conversion_totals.keyword_text) as keyword_text
    -- The pull writes Google's enum in title case, e.g. EXACT as Exact.
    , concat(
        upper(substr(coalesce(keyword_totals.keyword_match_type, conversion_totals.keyword_match_type), 1, 1))
        , lower(substr(coalesce(keyword_totals.keyword_match_type, conversion_totals.keyword_match_type), 2))
    ) as keyword_match_type
    , coalesce(keyword_totals.clicks, 0) as clicks
    , round(coalesce(keyword_totals.cost, 0), 2) as cost
    , round(coalesce(conversion_totals.all_conversions, 0), 2) as all_conversions
    , keyword_totals.currency_code
    , round(keyword_totals.keyword_first_page_cpc, 2) as keyword_first_page_cpc
from keyword_totals
full outer join conversion_totals
    on
        keyword_totals.course = conversion_totals.course
        and keyword_totals.report_date = conversion_totals.report_date
        and keyword_totals.campaign_name = conversion_totals.campaign_name
        and keyword_totals.keyword_text = conversion_totals.keyword_text
        and keyword_totals.keyword_match_type = conversion_totals.keyword_match_type
