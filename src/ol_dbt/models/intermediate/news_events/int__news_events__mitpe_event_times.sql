{#
  Start and end instants of every MIT Professional Education event, parsed the way
  MIT Learn's parse_date_time_range does (news_events/etl/utils.py). The feed gives a
  start date, an end date and a free-text time range ("12:30pm - 1pm",
  "9am - 10 am EDT", "8:30 AM - 11:30 AM").

  The time range is split into its "<hour>[:<minute>]<non-digits>" runs. With two
  runs they are the start and end times; with one, both; with none or more than two,
  both default to noon. A missing AM/PM is filled in from the other time, or set
  AM/PM when the start hour is after the end hour or the end is 12 and the start
  earlier (Learn's rule, which reads "12-1pm" as midnight to 1 PM). A time zone
  abbreviation applies its fixed offset, as dateparser does; without one the time is
  US Eastern. An end before the start becomes the start.

  Every past event is kept; integrations__learn__event_feed_items keeps the upcoming
  ones.
#}

with events as (
    select * from {{ ref('stg__mitpe__api__events') }}
)

, runs as (
    select
        event_id
        , coalesce(nullif(event_start_date_raw, ''), nullif(event_end_date_raw, '')) as start_date
        , coalesce(nullif(event_end_date_raw, ''), nullif(event_start_date_raw, '')) as end_date
        , regexp_extract_all(coalesce(event_time_range_raw, ''), '\d{1,2}(:\d{2})?\D*') as time_runs
    from events
)

, parsed_runs as (
    select
        event_id
        , start_date
        , end_date
        , {{ array_length('time_runs') }} as run_count
        {% for position in [1, 2] %}
        , cast(regexp_extract({{ element_at_array('time_runs', position) }}, '^(\d{1,2})', 1) as integer)
            as hour_{{ position }}
        , coalesce(
            {{ regexp_extract_or_null(element_at_array('time_runs', position), "'^\\d{1,2}:(\\d{2})'", 1) }}
            , '00'
        ) as minute_{{ position }}
        , lower({{ regexp_extract_or_null(element_at_array('time_runs', position), "'(?i)(am|pm)'", 1) }})
            as ampm_{{ position }}
        , upper(
            {{ regexp_extract_or_null(
                element_at_array('time_runs', position), "'(?i)(?:am|pm)\\s*([A-Za-z]{2,3})'", 1
            ) }}
        ) as tz_{{ position }}
        {% endfor %}
    from runs
)

, resolved as (
    select
        event_id
        , start_date
        , end_date
        , case when run_count in (1, 2) then hour_1 else 12 end as start_hour
        , case when run_count in (1, 2) then minute_1 else '00' end as start_minute
        , case when run_count = 2 then hour_2 when run_count = 1 then hour_1 else 12 end as end_hour
        , case when run_count = 2 then minute_2 when run_count = 1 then minute_1 else '00' end as end_minute
        , case
            when run_count = 2 and (hour_1 > hour_2 or (hour_2 = 12 and hour_1 < 12))
                then coalesce(ampm_1, 'am')
            when run_count = 2 then coalesce(ampm_1, ampm_2)
            when run_count = 1 then ampm_1
            else 'pm'
        end as start_ampm
        , case
            when run_count = 2 and (hour_1 > hour_2 or (hour_2 = 12 and hour_1 < 12))
                then coalesce(ampm_2, 'pm')
            when run_count = 2 then coalesce(ampm_2, ampm_1)
            when run_count = 1 then ampm_1
            else 'pm'
        end as end_ampm
        , case
            when run_count = 2 then coalesce(tz_1, tz_2)
            when run_count = 1 then tz_1
        end as start_tz
        , case
            when run_count = 2 then coalesce(tz_2, tz_1)
            when run_count = 1 then tz_1
        end as end_tz
    from parsed_runs
)

, clock as (
    select
        *
        {% for side in ['start', 'end'] %}
        -- 12 AM is midnight and 12 PM noon; an hour past 12 is already 24-hour
        -- time whatever the suffix says, as dateparser reads "13:00 PM".
        , case
            when {{ side }}_ampm = 'am' and {{ side }}_hour = 12 then 0
            when {{ side }}_ampm = 'pm' and {{ side }}_hour < 12 then {{ side }}_hour + 12
            else {{ side }}_hour
        end as {{ side }}_hour_24
        {% endfor %}
    from resolved
    where start_date is not null
)

, local_times as (
    select
        event_id
        {% for side in ['start', 'end'] %}
        -- A time dateparser cannot read ("40 minutes" gives hour 40) falls back to
        -- noon, as Learn's parse falls back to "<date> 12:00 PM".
        , concat(
            {{ side }}_date, ' '
            , case
                when {{ side }}_hour_24 > 23 or cast({{ side }}_minute as integer) > 59 then '12:00'
                else concat(lpad(cast({{ side }}_hour_24 as varchar), 2, '0'), ':', {{ side }}_minute)
            end
            , ':00'
        ) as {{ side }}_local
        -- dateparser applies an abbreviation's fixed offset, so EST and ET in
        -- summer are still UTC-5. The Etc zones' signs are inverted by POSIX
        -- convention. CT and MT are left out: dateparser recognizes CT without
        -- applying it, so Learn's result depends on its server's zone, and fails
        -- on MT.
        , case {{ side }}_tz
            when 'EST' then 'Etc/GMT+5'
            when 'ET' then 'Etc/GMT+5'
            when 'EDT' then 'Etc/GMT+4'
            when 'CST' then 'Etc/GMT+6'
            when 'CDT' then 'Etc/GMT+5'
            when 'MST' then 'Etc/GMT+7'
            when 'MDT' then 'Etc/GMT+6'
            when 'PST' then 'Etc/GMT+8'
            when 'PT' then 'Etc/GMT+8'
            when 'PDT' then 'Etc/GMT+7'
            when 'JST' then 'Etc/GMT-9'
            when 'KST' then 'Etc/GMT-9'
            when 'UTC' then 'UTC'
            when 'GMT' then 'UTC'
            else 'America/New_York'
        end as {{ side }}_zone
        {% endfor %}
    from clock
)

, instants as (
    select
        event_id
        , {{ local_timestamp_to_timestamptz('start_local', 'start_zone') }} as start_on
        , {{ local_timestamp_to_timestamptz('end_local', 'end_zone') }} as end_on
    from local_times
)

select
    event_id
    , start_on
    , case when end_on < start_on then start_on else end_on end as end_on
from instants
