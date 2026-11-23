{#
  The pieces of every MIT Professional Education event's dates and free-text time
  range ("12:30pm - 1pm", "9am - 10 am EDT", "7:45 AM - 5:30 PM (EDT, UTC-4)"), for
  the two parses that read them: int__news_events__mitpe_event_times (the
  platform's) and int__news_events__mitpe_event_times_learn (MIT Learn's). A table
  of its own, so neither parse renders these regular expressions inside its own
  expressions: StarRocks rejects a query whose expression tree passes 10,000 nodes.

  A missing start or end date is filled from the other. A range is split into its
  "<hour>[:<minute>]<non-digits>" runs, and the first two are read into hour,
  minute and AM/PM, twice:
  - learn_*: from the range as written, plus the zone abbreviation after an AM/PM,
    which is how Learn reads it.
  - unprefixed: from the range with any UTC or GMT offset taken out (it is
    utc_offset_hours and utc_offset_minutes instead) and "a.m."/"p.m." read as AM/PM, so "(EDT, UTC-4)"
    is not a third time and "8 p.m." is evening.
#}

with events as (
    select * from {{ ref('stg__mitpe__api__events') }}
)

, normalized as (
    select
        event_id
        , event_time_range_raw as time_range_raw
        , coalesce(nullif(event_start_date_raw, ''), nullif(event_end_date_raw, '')) as start_date
        , coalesce(nullif(event_end_date_raw, ''), nullif(event_start_date_raw, '')) as end_date
        , coalesce(event_time_range_raw, '') as learn_range
        -- "a.m." has no leading word boundary to match on: "8a.m." has none
        -- between the 8 and the a.
        , {{ regexp_replace_all(
            regexp_replace_all(
                regexp_replace_all(
                    "coalesce(event_time_range_raw, '')"
                    , "'(?i)\\b(?:utc|gmt)\\s*[+-]\\s*\\d{1,2}(?::?\\d{2})?'"
                    , "' '"
                )
                , "'(?i)a\\.\\s?m\\b\\.?'"
                , "'am'"
            )
            , "'(?i)p\\.\\s?m\\b\\.?'"
            , "'pm'"
        ) }} as platform_range
        , {{ regexp_extract_or_null("event_time_range_raw", "'(?i)\\b(?:utc|gmt)\\s*([+-])\\s*\\d{1,2}'", 1) }}
            as utc_offset_sign
        , {{ regexp_extract_or_null("event_time_range_raw", "'(?i)\\b(?:utc|gmt)\\s*[+-]\\s*(\\d{1,2})'", 1) }}
            as utc_offset_digits
        , {{ regexp_extract_or_null(
            "event_time_range_raw", "'(?i)\\b(?:utc|gmt)\\s*[+-]\\s*\\d{1,2}:?(\\d{2})'", 1
        ) }} as utc_offset_minute_digits
        -- Case-sensitive, so "et al." is not Eastern time.
        , {{ regexp_extract_or_null(
            "event_time_range_raw", "'\\b(ET|EST|EDT|CT|CST|CDT|MT|MST|MDT|PT|PST|PDT|JST|KST)\\b'", 1
        ) }} as zone_abbreviation
        , {{ regexp_extract_or_null("event_time_range_raw", "'\\b(UTC|GMT)\\b'", 1) }} as utc_name
    from events
)

, runs as (
    select
        *
        , {{ regexp_extract_all("platform_range", "'\\d{1,2}(:\\d{2})?\\D*'") }} as platform_runs
        , {{ regexp_extract_all("learn_range", "'\\d{1,2}(:\\d{2})?\\D*'") }} as learn_runs
    from normalized
)

select
    event_id
    , time_range_raw
    , start_date
    , end_date
    , zone_abbreviation
    , utc_name
    , cast(utc_offset_digits as integer) * case when utc_offset_sign = '-' then -1 else 1 end as utc_offset_hours
    , case
        when utc_offset_digits is not null then coalesce(cast(utc_offset_minute_digits as integer), 0)
    end as utc_offset_minutes
    {% for prefix, runs in [('', 'platform_runs'), ('learn_', 'learn_runs')] %}
    , {{ array_length(runs) }} as {{ prefix }}run_count
    {% for position in [1, 2] %}
    , cast({{ regexp_extract_or_null(element_at_array(runs, position), "'^(\\d{1,2})'", 1) }} as integer)
        as {{ prefix }}hour_{{ position }}
    , coalesce(
        {{ regexp_extract_or_null(element_at_array(runs, position), "'^\\d{1,2}:(\\d{2})'", 1) }}
        , '00'
    ) as {{ prefix }}minute_{{ position }}
    , lower({{ regexp_extract_or_null(element_at_array(runs, position), "'(?i)(am|pm)'", 1) }})
        as {{ prefix }}ampm_{{ position }}
    {% endfor %}
    {% endfor %}
    {% for position in [1, 2] %}
    , upper(
        {{ regexp_extract_or_null(
            element_at_array('learn_runs', position), "'(?i)(?:am|pm)\\s*([A-Za-z]{2,3})'", 1
        ) }}
    ) as learn_tz_{{ position }}
    {% endfor %}
from runs
