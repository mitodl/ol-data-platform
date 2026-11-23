{#
  Start and end of every MIT Professional Education event, from the feed's start
  date, end date and free-text time range ("12:30pm - 1pm", "9am - 10 am EDT",
  "8:30 AM - 11:30 AM"), split into parts by int__news_events__mitpe_event_time_parts.
  An event with neither date is dropped.

  This is the platform's parse. int__news_events__mitpe_event_times_learn keeps MIT
  Learn's, which differs on the cases below, for the Learn event feed view only.
  - Two time runs are the start and end; one is both.
  - A time without AM/PM takes the other time's, unless that puts the start after
    the end and the opposite one does not: "12-1pm" is noon to 1 PM, "11-1pm" 11 AM
    to 1 PM, "11am-1" 11 AM to 1 PM. With neither, the hours are 24-hour time.
  - "a.m." and "p.m." are AM and PM.
  - A US zone abbreviation anywhere in the range picks its region's zone, daylight
    time included, so "10am PT" in July is 17:00 UTC and "EST" in July is read as
    Eastern time. JST and KST are their zones. Otherwise a UTC or GMT offset
    ("EEST (UTC+3)") is that fixed offset, and a bare UTC or GMT is UTC. Without
    any the time is US Eastern, as it is for an abbreviation not listed here.
  - An empty range has time_parse_status no_time, and one that cannot be read
    (no time, more than two, an hour past 23 such as "40 minutes") unparseable.
    Both have null start_on and end_on; the dates are still there.
  - An end before the start is kept as written, with time_parse_status
    end_before_start.
#}

{%- set to_24 = "(case when {s} = 'am' and {h} = 12 then 0 when {s} = 'pm' and {h} < 12 then {h} + 12 else {h} end)" -%}
{%- set opposite = "(case {s} when 'am' then 'pm' when 'pm' then 'am' end)" -%}

with parts as (
    select * from {{ ref('int__news_events__mitpe_event_time_parts') }}
    where start_date is not null
)

, sides as (
    select
        event_id
        , start_date
        , end_date
        , time_range_raw
        , run_count
        , zone_abbreviation
        , utc_offset_hours
        , hour_1 as start_hour
        , cast(minute_1 as integer) as start_minute
        , ampm_1 as start_ampm
        , case when run_count = 2 then hour_2 else hour_1 end as end_hour
        , cast(case when run_count = 2 then minute_2 else minute_1 end as integer) as end_minute
        , case when run_count = 2 then ampm_2 else ampm_1 end as end_ampm
    from parts
)

{%- set start_minutes = "{} * 60 + start_minute" -%}
{%- set end_minutes = "{} * 60 + end_minute" -%}

, suffixes as (
    select
        *
        , case
            when start_ampm is not null or end_ampm is null then start_ampm
            when {{ start_minutes.format(to_24.format(h='start_hour', s='end_ampm')) }}
                <= {{ end_minutes.format(to_24.format(h='end_hour', s='end_ampm')) }}
                then end_ampm
            when {{ start_minutes.format(to_24.format(h='start_hour', s=opposite.format(s='end_ampm'))) }}
                <= {{ end_minutes.format(to_24.format(h='end_hour', s='end_ampm')) }}
                then {{ opposite.format(s='end_ampm') }}
            else end_ampm
        end as start_suffix
        , case
            when end_ampm is not null or start_ampm is null then end_ampm
            when {{ end_minutes.format(to_24.format(h='end_hour', s='start_ampm')) }}
                >= {{ start_minutes.format(to_24.format(h='start_hour', s='start_ampm')) }}
                then start_ampm
            when {{ end_minutes.format(to_24.format(h='end_hour', s=opposite.format(s='start_ampm'))) }}
                >= {{ start_minutes.format(to_24.format(h='start_hour', s='start_ampm')) }}
                then {{ opposite.format(s='start_ampm') }}
            else start_ampm
        end as end_suffix
    from sides
)

, clock as (
    select
        *
        , {{ to_24.format(h='start_hour', s='start_suffix') }} as start_hour_24
        , {{ to_24.format(h='end_hour', s='end_suffix') }} as end_hour_24
    from suffixes
)

, statuses as (
    select
        *
        , case
            when run_count = 0 and trim(coalesce(time_range_raw, '')) = '' then 'no_time'
            when run_count not in (1, 2) then 'unparseable'
            when start_hour_24 > 23 or end_hour_24 > 23 or start_minute > 59 or end_minute > 59 then 'unparseable'
            else 'parsed'
        end as read_status
        , case
            when zone_abbreviation in ('ET', 'EST', 'EDT') then 'America/New_York'
            when zone_abbreviation in ('CT', 'CST', 'CDT') then 'America/Chicago'
            when zone_abbreviation in ('MT', 'MST', 'MDT') then 'America/Denver'
            when zone_abbreviation in ('PT', 'PST', 'PDT') then 'America/Los_Angeles'
            when zone_abbreviation = 'JST' then 'Asia/Tokyo'
            when zone_abbreviation = 'KST' then 'Asia/Seoul'
            -- "UTC+3" is Etc/GMT-3: POSIX inverts the sign.
            when utc_offset_hours is not null
                then concat(
                    'Etc/GMT', case when utc_offset_hours > 0 then '-' else '+' end
                    , cast(abs(utc_offset_hours) as varchar)
                )
            when zone_abbreviation in ('UTC', 'GMT') then 'UTC'
            else 'America/New_York'
        end as time_zone
    from clock
)

, local_times as (
    select
        event_id
        , start_date
        , end_date
        , read_status
        , time_zone
        {% for side in ['start', 'end'] %}
        , case
            when read_status = 'parsed'
                then concat(
                    {{ side }}_date, ' '
                    , lpad(cast({{ side }}_hour_24 as varchar), 2, '0'), ':'
                    , lpad(cast({{ side }}_minute as varchar), 2, '0'), ':00'
                )
        end as {{ side }}_local
        {% endfor %}
    from statuses
)

, instants as (
    select
        event_id
        , start_date
        , end_date
        , read_status
        , time_zone
        , {{ local_timestamp_to_timestamptz('start_local', 'time_zone') }} as start_on
        , {{ local_timestamp_to_timestamptz('end_local', 'time_zone') }} as end_on
    from local_times
)

-- Set to UTC so the value reads the same on every engine: Trino keeps the zone a
-- timestamp was built in.
select
    event_id
    , cast(start_date as date) as event_start_date
    , cast(end_date as date) as event_end_date
    , {{ timestamptz_at_utc('start_on') }} as start_on
    , {{ timestamptz_at_utc('end_on') }} as end_on
    , case when read_status = 'parsed' and end_on < start_on then 'end_before_start' else read_status end
        as time_parse_status
    , time_zone
from instants
