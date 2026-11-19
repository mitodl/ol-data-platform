-- ad_opt_v2's parse_change_history (campaign_intervals, build_summary): each Search campaign's
-- state on every day from its first click, replayed from its changes, collapsed into versions.
{%- set positive_types = ['broad', 'phrase', 'exact'] %}
{%- set list_types = positive_types + ['negative_broad', 'negative_phrase', 'negative_exact'] %}

with keyword_days as (
    select course, campaign_name, report_date, keyword_text, keyword_match_type
    from {{ ref('int__google_ads__keyword_daily') }}
    where keyword_text != ''
)

, campaigns as (
    select course, campaign_name, min(report_date) as first_day
    from keyword_days
    where campaign_name like '%Search%' and campaign_name not like '%Experiment%'
    group by course, campaign_name
)

, course_last_days as (
    select course, max(report_date) as last_day
    from keyword_days
    group by course
)

, campaign_days as (
    select
        campaigns.course
        , campaigns.campaign_name
        , cast({{ dbt.dateadd('day', 'days.day_number - 1', 'campaigns.first_day') }} as date) as report_date
    from campaigns
    inner join course_last_days on campaigns.course = course_last_days.course
    cross join
        {{ unnest_sequence(
            date_diff("'day'", 'campaigns.first_day', 'course_last_days.last_day') ~ ' + 1', 'days', 'day_number'
        ) }}
)

-- A change applies from its day in the account's zone, as the replay compares timestamp.date().
, changes as (
    select
        course
        , campaign_name
        , change_at
        , {{ timestamptz_to_local_date('change_at', google_ads_default_time_zone()) }} as change_date
        , change_type
        , coalesce(ad_group_name, '') as ad_group_name
        , keyword_action
        , keyword_text
        , keyword_match_type
        , previous_value
        , new_value
    from {{ ref('int__google_ads__campaign_changes') }}
)

, setting_changes as (
    select
        course
        , campaign_name
        , change_at
        , change_date
        , case change_type
            when 'budget' then 'budget'
            when 'campaign_status' then 'status'
            when 'campaign_start' then 'start'
            when 'campaign_end' then 'end'
        end as setting
        , case
            when change_type = 'budget' then cast(cast(nullif(previous_value, '') as decimal(12, 2)) as varchar)
            when change_type = 'campaign_status' and previous_value = 'enabled' then 'active'
            else previous_value
        end as previous_value
        , case
            when change_type = 'budget' then cast(cast(nullif(new_value, '') as decimal(12, 2)) as varchar)
            when change_type = 'campaign_status' and new_value = 'enabled' then 'active'
            else new_value
        end as new_value
    from changes
    where change_type in ('budget', 'campaign_status', 'campaign_start', 'campaign_end')
)

-- Before its first change, a setting has that change's "changed from" value (initial_state).
, initial_settings as (
    select course, campaign_name, setting, cast('1900-01-01' as date) as valid_from, previous_value as setting_value
    from (
        select
            course
            , campaign_name
            , setting
            , previous_value
            , row_number() over (partition by course, campaign_name, setting order by change_at) as change_rank
        from setting_changes
    ) as ranked
    where change_rank = 1 and previous_value is not null
)

, daily_settings as (
    select course, campaign_name, setting, change_date as valid_from, new_value as setting_value
    from (
        select
            course
            , campaign_name
            , setting
            , change_date
            , new_value
            , row_number() over (
                partition by course, campaign_name, setting, change_date order by change_at desc
            ) as change_rank
        from setting_changes
    ) as ranked
    where change_rank = 1
)

-- Each keyword, negative keyword or ad group status change starts a new keyword version.
, daily_versions as (
    select
        course
        , campaign_name
        , case when change_type = 'negative_keyword' then 'negative_version' else 'keyword_version' end as setting
        , change_date as valid_from
        , cast(sum(count(*)) over (
            partition by
                course
                , campaign_name
                , case when change_type = 'negative_keyword' then 'negative_version' else 'keyword_version' end
            order by change_date
            rows unbounded preceding
        ) as varchar) as setting_value
    from changes
    where change_type in ('keyword', 'negative_keyword', 'ad_group_status')
    group by
        course
        , campaign_name
        , case when change_type = 'negative_keyword' then 'negative_version' else 'keyword_version' end
        , change_date
)

, setting_intervals as (
    select
        course
        , campaign_name
        , setting
        , valid_from
        , lead(valid_from) over (partition by course, campaign_name, setting order by valid_from) as valid_to
        , setting_value
    from (
        select * from initial_settings
        union all
        select * from daily_settings
        union all
        select * from daily_versions
    ) as settings
)

, day_states as (
    select
        campaign_days.course
        , campaign_days.campaign_name
        , campaign_days.report_date
        , cast(max(case when setting_intervals.setting = 'budget' then setting_intervals.setting_value end)
            as decimal(12, 2)) as daily_budget
        , coalesce(max(case when setting_intervals.setting = 'status' then setting_intervals.setting_value end), 'active')
        as campaign_status
        , cast(max(case when setting_intervals.setting = 'start' then setting_intervals.setting_value end) as date)
        as campaign_start_date
        , cast(max(case when setting_intervals.setting = 'end' then setting_intervals.setting_value end) as date)
        as campaign_end_date
        , coalesce(max(case when setting_intervals.setting = 'keyword_version' then setting_intervals.setting_value end), '0')
        as keyword_version
        , coalesce(max(case when setting_intervals.setting = 'negative_version' then setting_intervals.setting_value end), '0')
        as negative_version
    from campaign_days
    left join setting_intervals
        on
            campaign_days.course = setting_intervals.course
            and campaign_days.campaign_name = setting_intervals.campaign_name
            and campaign_days.report_date >= setting_intervals.valid_from
            and (setting_intervals.valid_to is null or campaign_days.report_date < setting_intervals.valid_to)
    group by campaign_days.course, campaign_days.campaign_name, campaign_days.report_date
)

, day_serving as (
    select
        course
        , campaign_name
        , report_date
        , daily_budget
        , keyword_version
        , negative_version
        , case
            when campaign_status != 'active' then campaign_status
            when campaign_start_date is not null and report_date < campaign_start_date then 'not_started'
            when campaign_end_date is not null and report_date > campaign_end_date then 'ended'
            else 'active'
        end as serving_status
    from day_states
)

, day_changes as (
    select
        day_serving.*
        , lag(day_serving.report_date) over (partition by day_serving.course, day_serving.campaign_name order by day_serving.report_date) is null as is_first_day
        , coalesce(cast(lag(day_serving.daily_budget) over (partition by day_serving.course, day_serving.campaign_name order by day_serving.report_date) as varchar), '')
        != coalesce(cast(day_serving.daily_budget as varchar), '') as budget_changed
        , lag(day_serving.serving_status) over (partition by day_serving.course, day_serving.campaign_name order by day_serving.report_date) != day_serving.serving_status
        as serving_changed
        , lag(day_serving.keyword_version) over (partition by day_serving.course, day_serving.campaign_name order by day_serving.report_date) != day_serving.keyword_version
        as keywords_changed
        , lag(day_serving.negative_version) over (partition by day_serving.course, day_serving.campaign_name order by day_serving.report_date) != day_serving.negative_version
        as negatives_changed
    from day_serving
)

, day_versions as (
    select
        day_changes.*
        , sum(case
            when day_changes.is_first_day or day_changes.budget_changed or day_changes.serving_changed or day_changes.keywords_changed or day_changes.negatives_changed then 1
            else 0
        end) over (
            partition by day_changes.course, day_changes.campaign_name order by day_changes.report_date rows unbounded preceding
        ) as version_number
        , case
            when day_changes.is_first_day then 'first_seen'
            else nullif(concat_ws(
                ';'
                , case when day_changes.budget_changed then 'daily_budget' end
                , case when day_changes.serving_changed then concat('campaign_', day_changes.serving_status) end
                , case when day_changes.keywords_changed then 'keywords' end
                , case when day_changes.negatives_changed then 'negative_keywords' end
            ), '')
        end as change_type
    from day_changes
)

, intervals as (
    select
        course
        , campaign_name
        , version_number
        , min(report_date) as start_date
        , max(report_date) as last_day
        , max(daily_budget) as daily_budget
        , max(serving_status) as serving_status
        , max(keyword_version) as keyword_version
        , max(change_type) as change_type
    from day_versions
    group by course, campaign_name, version_number
)

, interval_rows as (
    select intervals.course, intervals.campaign_name, intervals.version_number, count(keyword_days.report_date) as panel_rows
    from intervals
    left join keyword_days
        on
            intervals.course = keyword_days.course
            and intervals.campaign_name = keyword_days.campaign_name
            and keyword_days.report_date between intervals.start_date and intervals.last_day
    group by intervals.course, intervals.campaign_name, intervals.version_number
)

-- Days not serving and without clicks are gaps between runs, and a version needs a known budget.
-- end_date is the next version's start, taken before the filter below, as the replay does.
, kept_intervals as (
    select
        intervals.*
        , lead(intervals.start_date) over (
            partition by intervals.course, intervals.campaign_name order by intervals.start_date
        ) as end_date
    from intervals
)

, versions as (
    select
        kept_intervals.*
        , case when created.campaign_name is not null then 'configured' else 'clicked' end as keyword_source
        , row_number() over (
            partition by kept_intervals.course order by kept_intervals.campaign_name, kept_intervals.start_date
        ) as campaign_version
    from kept_intervals
    inner join interval_rows
        on
            kept_intervals.course = interval_rows.course
            and kept_intervals.campaign_name = interval_rows.campaign_name
            and kept_intervals.version_number = interval_rows.version_number
    -- A campaign created inside the history has its full keyword list there.
    left join (
        select distinct course, campaign_name
        from changes
        where change_type = 'campaign_status' and previous_value is null
    ) as created
        on kept_intervals.course = created.course and kept_intervals.campaign_name = created.campaign_name
    where
        not (kept_intervals.serving_status != 'active' and interval_rows.panel_rows = 0)
        and kept_intervals.daily_budget is not null
)

, keyword_events as (
    select
        course
        , campaign_name
        , ad_group_name
        , change_type
        , lower(keyword_match_type) as match_type
        , keyword_text
        , keyword_action
        , change_at
        , change_date
    from changes
    where change_type in ('keyword', 'negative_keyword') and keyword_text != ''
)

, ad_group_events as (
    select course, campaign_name, ad_group_name, new_value as ad_group_status, change_at, change_date
    from changes
    where change_type = 'ad_group_status'
)

-- Each ad group's last status by the version's first day; unset means active.
, ad_group_statuses as (
    select course, campaign_version, ad_group_name, ad_group_status
    from (
        select
            versions.course
            , versions.campaign_version
            , ad_group_events.ad_group_name
            , ad_group_events.ad_group_status
            , row_number() over (
                partition by versions.course, versions.campaign_version, ad_group_events.ad_group_name
                order by ad_group_events.change_at desc
            ) as status_rank
        from versions
        inner join ad_group_events
            on
                versions.course = ad_group_events.course
                and versions.campaign_name = ad_group_events.campaign_name
                and ad_group_events.change_date <= versions.start_date
        where versions.keyword_source = 'configured'
    ) as ranked
    where status_rank = 1
)

-- Configured keywords: the last action on each ad group keyword by the version's first day,
-- in ad groups that are not paused or removed.
, configured_keywords as (
    select
        last_actions.course
        , last_actions.campaign_version
        , last_actions.match_type as list_type
        , last_actions.keyword_text
    from (
        select
            versions.course
            , versions.campaign_version
            , keyword_events.ad_group_name
            , keyword_events.match_type
            , keyword_events.keyword_text
            , keyword_events.keyword_action
            , row_number() over (
                partition by
                    versions.course
                    , versions.campaign_version
                    , keyword_events.ad_group_name
                    , keyword_events.match_type
                    , keyword_events.keyword_text
                order by keyword_events.change_at desc
            ) as action_rank
        from versions
        inner join keyword_events
            on
                versions.course = keyword_events.course
                and versions.campaign_name = keyword_events.campaign_name
                and keyword_events.change_date <= versions.start_date
        where versions.keyword_source = 'configured' and keyword_events.change_type = 'keyword'
    ) as last_actions
    left join ad_group_statuses
        on
            last_actions.course = ad_group_statuses.course
            and last_actions.campaign_version = ad_group_statuses.campaign_version
            and last_actions.ad_group_name = ad_group_statuses.ad_group_name
    where
        last_actions.action_rank = 1
        and last_actions.keyword_action in ('added', 'enabled')
        and coalesce(ad_group_statuses.ad_group_status, 'enabled') not in ('paused', 'removed')
)

-- Clicked keywords: keywords with panel rows, pooled over days with the same keyword version.
, clicked_keywords as (
    select versions.course, versions.campaign_version, lower(clicked.keyword_match_type) as list_type, clicked.keyword_text
    from versions
    inner join (
        select distinct
            keyword_days.course
            , keyword_days.campaign_name
            , day_states.keyword_version
            , keyword_days.keyword_match_type
            , keyword_days.keyword_text
        from keyword_days
        inner join day_states
            on
                keyword_days.course = day_states.course
                and keyword_days.campaign_name = day_states.campaign_name
                and keyword_days.report_date = day_states.report_date
    ) as clicked
        on
            versions.course = clicked.course
            and versions.campaign_name = clicked.campaign_name
            and versions.keyword_version = clicked.keyword_version
    where versions.keyword_source = 'clicked'
)

, negative_keywords as (
    select course, campaign_version, concat('negative_', match_type) as list_type, keyword_text
    from (
        select
            versions.course
            , versions.campaign_version
            , keyword_events.match_type
            , keyword_events.keyword_text
            , keyword_events.keyword_action
            , row_number() over (
                partition by
                    versions.course, versions.campaign_version, keyword_events.match_type, keyword_events.keyword_text
                order by keyword_events.change_at desc
            ) as action_rank
        from versions
        inner join keyword_events
            on
                versions.course = keyword_events.course
                and versions.campaign_name = keyword_events.campaign_name
                and keyword_events.change_date <= versions.start_date
        where keyword_events.change_type = 'negative_keyword'
    ) as last_actions
    where action_rank = 1 and keyword_action in ('added', 'enabled')
)

, version_keywords as (
    select * from configured_keywords
    union all
    select * from clicked_keywords
    union all
    select * from negative_keywords
)

, version_lists as (
    select
        course
        , campaign_version
        {%- for list_type in list_types %}
        , coalesce({{ array_join(
            array_agg_filter('keyword_text', "list_type = '" ~ list_type ~ "'", order_by='keyword_text', distinct=true)
            , '; '
        ) }}, '') as {{ list_type }}_keywords
        , count(distinct case when list_type = '{{ list_type }}' then keyword_text end) as {{ list_type }}_keyword_count
        {%- endfor %}
        , coalesce({{ array_join(
            array_agg_filter('keyword_text', "list_type in ('broad', 'phrase', 'exact')", order_by='keyword_text', distinct=true)
            , '; '
        ) }}, '') as unique_keywords
        , count(distinct case when list_type in ('broad', 'phrase', 'exact') then keyword_text end) as unique_keyword_count
        , coalesce({{ array_join(
            array_agg_filter('keyword_text', "list_type like 'negative_%'", order_by='keyword_text', distinct=true)
            , '; '
        ) }}, '') as negative_keywords
        , count(distinct case when list_type like 'negative_%' then keyword_text end) as negative_keyword_count
    from version_keywords
    group by course, campaign_version
)

, versions_with_lists as (
    select
        versions.course
        , versions.campaign_version
        , versions.campaign_name
        , versions.start_date
        , versions.end_date
        , versions.serving_status
        , versions.change_type
        , versions.daily_budget
        , versions.keyword_source
        {%- for list_type in list_types %}
        , coalesce(version_lists.{{ list_type }}_keywords, '') as {{ list_type }}_keywords
        , coalesce(version_lists.{{ list_type }}_keyword_count, 0) as {{ list_type }}_keyword_count
        {%- endfor %}
        , coalesce(version_lists.unique_keywords, '') as unique_keywords
        , coalesce(version_lists.unique_keyword_count, 0) as unique_keyword_count
        , coalesce(version_lists.negative_keywords, '') as negative_keywords
        , coalesce(version_lists.negative_keyword_count, 0) as negative_keyword_count
    from versions
    left join version_lists
        on versions.course = version_lists.course and versions.campaign_version = version_lists.campaign_version
)

-- Versions with the same keyword lists share a keyword set, numbered by first use.
, keyword_sets as (
    select
        course
        , broad_keywords
        , phrase_keywords
        , exact_keywords
        , negative_broad_keywords
        , negative_phrase_keywords
        , negative_exact_keywords
        , concat('ks_', lpad(cast(row_number() over (partition by course order by min(campaign_version)) as varchar), 4, '0'))
        as keyword_set_id
    from versions_with_lists
    group by
        course
        , broad_keywords
        , phrase_keywords
        , exact_keywords
        , negative_broad_keywords
        , negative_phrase_keywords
        , negative_exact_keywords
)

select
    versions_with_lists.course
    , versions_with_lists.campaign_version
    , keyword_sets.keyword_set_id
    , versions_with_lists.campaign_name
    , {{ google_ads_campaign_region('versions_with_lists.campaign_name') }} as campaign_region
    , versions_with_lists.start_date
    , versions_with_lists.end_date
    , versions_with_lists.serving_status as campaign_serving_status
    , versions_with_lists.change_type
    , versions_with_lists.daily_budget
    , concat_ws(
        '; '
        , case when versions_with_lists.broad_keyword_count > 0 then 'Broad' end
        , case when versions_with_lists.phrase_keyword_count > 0 then 'Phrase' end
        , case when versions_with_lists.exact_keyword_count > 0 then 'Exact' end
    ) as match_types
    , versions_with_lists.keyword_source
    , versions_with_lists.unique_keyword_count
    , versions_with_lists.broad_keyword_count
    , versions_with_lists.phrase_keyword_count
    , versions_with_lists.exact_keyword_count
    , versions_with_lists.negative_keyword_count
    {%- for list_type in list_types %}
    , versions_with_lists.{{ list_type }}_keywords
    {%- endfor %}
    , versions_with_lists.unique_keywords
    , versions_with_lists.negative_keywords
from versions_with_lists
inner join keyword_sets
    on
        versions_with_lists.course = keyword_sets.course
        and versions_with_lists.broad_keywords = keyword_sets.broad_keywords
        and versions_with_lists.phrase_keywords = keyword_sets.phrase_keywords
        and versions_with_lists.exact_keywords = keyword_sets.exact_keywords
        and versions_with_lists.negative_broad_keywords = keyword_sets.negative_broad_keywords
        and versions_with_lists.negative_phrase_keywords = keyword_sets.negative_phrase_keywords
        and versions_with_lists.negative_exact_keywords = keyword_sets.negative_exact_keywords
