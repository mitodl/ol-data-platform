{#
  Start and end instants of every MIT Professional Education event as MIT Learn's
  parse_date_time_range (news_events/etl/utils.py) reads them, defects included.
  Only integrations__learn__event_feed_items reads it, for parity with Learn.
  Every other consumer reads int__news_events__mitpe_event_times, the platform's
  parse, which differs where Learn's is wrong:
  - With two time runs they are the start and end times; with one, both; with
    none or more than two, both default to noon, as does an hour dateparser cannot
    read ("40 minutes").
  - A missing AM/PM is filled in from the other time, or set AM/PM when the start
    hour is after the end hour or the end is 12 and the start earlier, which reads
    "12-1pm" as midnight to 1 PM.
  - A time zone abbreviation after an AM/PM applies its fixed offset, as dateparser
    does, so "ET" and "PT" in summer are an hour off. Without one the time is US
    Eastern.
  - An end before the start becomes the start.

  Every past event is kept; integrations__learn__event_feed_items keeps the upcoming
  ones.
#}

with parts as (
    select
        event_id
        , start_date
        , end_date
        , learn_run_count as run_count
        {% for position in [1, 2] %}
        , learn_hour_{{ position }} as hour_{{ position }}
        , learn_minute_{{ position }} as minute_{{ position }}
        , learn_ampm_{{ position }} as ampm_{{ position }}
        , learn_tz_{{ position }} as tz_{{ position }}
        {% endfor %}
    from {{ ref('int__news_events__mitpe_event_time_parts') }}
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
    from parts
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

-- Set to UTC so the value reads the same on every engine: Trino keeps the zone a
-- timestamp was built in, and a consumer or test that prints it would otherwise
-- see "12:30-04:00" on Trino and "16:30+00:00" on DuckDB for one instant.
select
    event_id
    , {{ timestamptz_at_utc('start_on') }} as start_on
    , {{ timestamptz_at_utc('case when end_on < start_on then start_on else end_on end') }} as end_on
from instants
