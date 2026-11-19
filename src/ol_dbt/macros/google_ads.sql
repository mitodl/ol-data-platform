{% macro google_ads_micros(column_name) -%}
    {#- Google Ads reports money in millionths of the account currency. -#}
    cast({{ column_name }} as decimal(38, 6)) / 1000000
{%- endmacro %}


{% macro google_ads_current_rows(raw_table, date_column, key_columns) %}
    {#- `current_source`: each key's row from the latest fetch of its date in fetch_log. -#}
    , fetch_windows as (
        select
            customer_id
            , cast(window_start as date) as window_start
            , cast(window_end as date) as window_end
            , retrieved_at
        from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__fetch_log') }}
        where table_name = '{{ raw_table }}'
    )

    , fetch_days as (
        select
            fetch_windows.customer_id
            , fetch_windows.retrieved_at
            , cast({{ dbt.dateadd('day', 'days.day_offset - 1', 'fetch_windows.window_start') }} as date)
            as fetched_date
        from fetch_windows
        cross join
            {{ unnest_sequence(
                date_diff("'day'", 'fetch_windows.window_start', 'fetch_windows.window_end') ~ ' + 1'
                , 'days'
                , 'day_offset'
            ) }}
    )

    , latest_fetch as (
        select
            customer_id
            , fetched_date
            , max(retrieved_at) as retrieved_at
        from fetch_days
        group by customer_id, fetched_date
    )

    , fetched_rows as (
        select
            source.*
            -- A load appended twice has two copies from one fetch; keep the last load's.
            , row_number() over (
                partition by {% for column in key_columns %}source.{{ column }}{{ ", " if not loop.last }}{% endfor %}
                order by source._dlt_load_id desc
            ) as key_rank
        from source
        inner join latest_fetch
            on
                source.customer_id = latest_fetch.customer_id
                and cast(source.{{ date_column }} as date) = latest_fetch.fetched_date
                and source.retrieved_at = latest_fetch.retrieved_at
    )

    , current_source as (
        select * from fetched_rows
        where key_rank = 1
    )
{% endmacro %}


{% macro google_ads_local_to_utc(local_timestamp, time_zone) -%}
    {#- Google Ads gives times in the account's zone with no offset. -#}
    {{ timestamptz_at_utc(local_timestamp_to_timestamptz(local_timestamp, time_zone)) }}
{%- endmacro %}

{% macro google_ads_default_time_zone() -%}
    {#- Every account's zone today. Used for the history files, which name no account,
        and for an account no longer under the manager. -#}
    'America/New_York'
{%- endmacro %}


{% macro google_ads_clean_keyword(keyword_expr) -%}
    {#- ad_opt_v2's clean_keyword_text: no quotes, brackets or +, one space between words, lowercase. -#}
    lower(trim({{ regexp_replace_all(regexp_replace_all(keyword_expr, "'[''\"+\\[\\]]'", "''"), "'\\s+'", "' '") }}))
{%- endmacro %}


{% macro google_ads_campaign_region(campaign_expr) -%}
    {#- ad_opt_v2's region rule: the first "-"-separated part naming USA/US, or A, B or C (optionally "Region A"). -#}
    {%- set pattern = "'(?:^|-)(?:[^-]*\\b(USA?)\\b|\\s*(?:Region )?([ABC])(?:[ /(]|\\s*(?:-|$)))'" -%}
    case
        when nullif(regexp_extract({{ campaign_expr }}, {{ pattern }}, 1), '') is not null then 'USA'
        else nullif(regexp_extract({{ campaign_expr }}, {{ pattern }}, 2), '')
    end
{%- endmacro %}


{% macro google_ads_is_search_campaign(campaign_name_expr, channel_type_expr) -%}
    {#- ad_opt_v2's shared campaign filter on every pull. -#}
    {{ channel_type_expr }} = 'SEARCH'
    and {{ campaign_name_expr }} not like 'EXCLUDE%'
    and {{ campaign_name_expr }} not like '%Experiment%'
{%- endmacro %}


{% macro google_ads_excluded_campaigns(source_relation) -%}
    {#- Campaigns whose name holds one of the course's excluded_campaign_patterns, ignoring case. -#}
    select distinct named.course, named.campaign_name
    from {{ source_relation }} as named
    inner join {{ ref('google_ads_course_settings') }} as excluded
        on
            named.course = excluded.course
            and excluded.setting = 'excluded_campaign_pattern'
            and lower(named.campaign_name) like concat('%', lower(excluded.setting_value), '%')
{%- endmacro %}
