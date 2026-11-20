{#
  One row per Sloan Executive Education course offering, with Sloan's labels, codes
  and text kept and parsed where they have a meaning of their own. Dates are calendar
  dates in US Eastern time. MIT Learn's shaping (delivery, availability, pace and
  format codes, USD for a missing or unknown currency, empty strings, the duration in
  weeks and its reading of the time commitment) is in integrations__learn__see_runs.
#}

with source_offerings as (
    select * from {{ ref('stg__see__api__course_offerings') }}
)

, courses as (
    select * from {{ ref('stg__see__api__courses') }}
)

, currencies as (
    select * from {{ ref('iso_4217_currencies') }}
)

, normalized_offerings as (
    select
        *
        , lower(trim(courseoffering_time_commitment)) as time_commitment_normalized
    from source_offerings
)

, offerings as (
    select
        *
        , {{ duration_parse_parts('courseoffering_duration') }}
        -- A number or range of hours at the start, e.g. "6-8 hours/week", "6–8 hours/week",
        -- "5 - 7 Hours/week", "8 hours, 5 days/week". "3 one-week modules" names no hours.
        , {{ regexp_extract_or_null(
            'time_commitment_normalized', "'^(\\d+(?:\\.\\d+)?)(?:\\s*(?:-|–|to)\\s*\\d+(?:\\.\\d+)?)?\\s*hours?'", 1
        ) }} as hours_min_value
        , {{ regexp_extract_or_null(
            'time_commitment_normalized', "'^\\d+(?:\\.\\d+)?\\s*(?:-|–|to)\\s*(\\d+(?:\\.\\d+)?)\\s*hours?'", 1
        ) }} as hours_max_value
        -- the period the hours are per, only when it follows them ("hours/week",
        -- "hours/ day", "hours per week")
        , {{ regexp_extract_or_null(
            'time_commitment_normalized'
            , "'^\\d+(?:\\.\\d+)?(?:\\s*(?:-|–|to)\\s*\\d+(?:\\.\\d+)?)?\\s*hours?\\s*(?:/|per)\\s*(week|day)'"
            , 1
        ) }} as hours_period
    from normalized_offerings
)

select
    offerings.course_id as readable_id
    , offerings.courseoffering_title as run_id
    , offerings.courseoffering_api_position as run_api_position
    , courses.course_title as title
    , courses.course_url as url
    , {{ local_date_to_timestamptz("nullif(offerings.courseoffering_start_date, '')", 'America/New_York') }}
        as start_on
    , {{ local_date_to_timestamptz("nullif(offerings.courseoffering_end_date, '')", 'America/New_York') }}
        as end_on
    , offerings.courseoffering_delivery as delivery_label
    , offerings.courseoffering_format as format_label
    -- Only an online, on-demand offering is self-paced and available anytime.
    , coalesce(
        offerings.courseoffering_delivery = 'Online'
        and offerings.courseoffering_format = 'Asynchronous (On-Demand)'
        , false
    ) as is_on_demand
    -- In-person offerings are synchronous and blended ones both. Otherwise the
    -- offering's format decides.
    , case offerings.courseoffering_delivery
        when 'In Person' then true
        when 'Blended' then true
        else coalesce(offerings.courseoffering_format, '') not like '%Asynchronous%'
    end as is_synchronous
    , case offerings.courseoffering_delivery
        when 'In Person' then false
        when 'Blended' then true
        else coalesce(offerings.courseoffering_format, '') like '%Asynchronous%'
    end as is_asynchronous
    , nullif(offerings.courseoffering_location, '') as location
    , cast(offerings.courseoffering_price as decimal(12, 2)) as price
    , nullif(offerings.courseoffering_currency, '') as currency
    , currencies.currency_code is not null as is_known_currency
    , {{ array_filter_nonempty(
        "split(" ~ regexp_replace_all(
            "trim(coalesce(offerings.courseoffering_faculty_names_raw, ''))", "'\\s*,\\s*'", "','"
        ) ~ ", ',')"
    ) }} as instructors
    , offerings.courseoffering_continuing_ed_credits as continuing_ed_credits
    , nullif(offerings.courseoffering_duration, '') as duration
    , {{ duration_min('offerings.') }} as duration_min
    , {{ duration_max('offerings.') }} as duration_max
    , {{ duration_unit('offerings.') }} as duration_unit
    , nullif(offerings.courseoffering_time_commitment, '') as time_commitment
    , {{ try_cast('offerings.hours_min_value', 'decimal(8, 2)') }} as time_commitment_min_hours
    , {{ try_cast('coalesce(offerings.hours_max_value, offerings.hours_min_value)', 'decimal(8, 2)') }}
        as time_commitment_max_hours
    , offerings.hours_period as time_commitment_hours_per
from offerings
left join courses on offerings.course_id = courses.course_id
left join currencies on offerings.courseoffering_currency = currencies.currency_code
