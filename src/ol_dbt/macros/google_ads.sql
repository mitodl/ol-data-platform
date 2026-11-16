{% macro google_ads_micros(column_name) -%}
    {#- Google Ads reports money in millionths of the account currency. -#}
    cast({{ column_name }} as decimal(38, 6)) / 1000000
{%- endmacro %}


{% macro google_ads_current_rows(raw_table, date_column) %}
    {#
        Emit a `current_source` CTE holding the rows of `source` from the latest fetch
        that covered their date (`date_column`) in raw__google_ads__api__fetch_log.

        A re-pull merges the rows Google still returns and leaves a row Google
        retracted with an older retrieved_at, because a merge never deletes. A
        snapshot run twice on one day appends both copies. Either way the stale row's
        retrieved_at is older than the newest fetch of its date, so it drops here.
        Fetch windows are expanded to days first, so rows join on (account, date).
    #}
    , fetch_windows as (
        select
            customer_id
            , cast(window_start as date) as window_start
            , cast(window_end as date) as window_end
            , retrieved_at
        from {{ source('ol_warehouse_raw_data', 'raw__google_ads__api__fetch_log') }}
        where table_name = '{{ raw_table }}'
    )

    , latest_fetch as (
        select
            fetch_windows.customer_id
            , cast({{ dbt.dateadd('day', 'fetch_days.day_offset - 1', 'fetch_windows.window_start') }} as date)
            as fetched_date
            , max(fetch_windows.retrieved_at) as retrieved_at
        from fetch_windows
        cross join
            {{ unnest_sequence(
                date_diff("'day'", 'fetch_windows.window_start', 'fetch_windows.window_end') ~ ' + 1'
                , 'fetch_days'
                , 'day_offset'
            ) }}
        group by
            fetch_windows.customer_id
            , cast({{ dbt.dateadd('day', 'fetch_days.day_offset - 1', 'fetch_windows.window_start') }} as date)
    )

    , current_source as (
        select source.*
        from source
        inner join latest_fetch
            on
                source.customer_id = latest_fetch.customer_id
                and cast(source.{{ date_column }} as date) = latest_fetch.fetched_date
                and source.retrieved_at = latest_fetch.retrieved_at
    )
{% endmacro %}


{% macro google_ads_mask_emails(column_name) -%}
    {#- The change history names people by email; staging keeps the change, not who. -#}
    {{ regexp_replace_all(column_name, "'[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)+'", "'<email>'") }}
{%- endmacro %}
