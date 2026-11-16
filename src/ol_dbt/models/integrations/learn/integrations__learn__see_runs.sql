{#
  integrations__learn__see_runs
  Runs of the Sloan Executive Education courses in integrations__learn__see_courses,
  one row per course offering, with the fields MIT Learn's legacy Sloan ETL set
  (transform_run in learning_resources/etl/sloan.py). Dates are ISO 8601 UTC instants
  of midnight US Eastern. int__see__course_runs keeps Sloan's labels; the codes,
  defaults and parses MIT Learn applied are here.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select readable_id from {{ ref('int__see__courses') }}
    where has_url and has_offerings
)

select
    runs.readable_id
    , runs.run_id
    , runs.title
    , runs.url
    , {{ format_timestamp_as_iso8601('runs.start_on') }} as start_date
    , {{ format_timestamp_as_iso8601('runs.end_on') }} as end_date
    , {{ array_of([learn_delivery('runs.delivery_label')]) }} as delivery
    , case when runs.is_on_demand then 'anytime' else 'dated' end as availability
    , {{ array_of(["case when runs.is_on_demand then 'self_paced' else 'instructor_paced' end"]) }} as pace
    -- Synchronous first, as parse_format lists a Blended run's formats.
    , split(concat_ws(
        ','
        , case when runs.is_synchronous then 'synchronous' end
        , case when runs.is_asynchronous then 'asynchronous' end
    ), ',') as format
    , case when runs.delivery_label = 'Online' then '' else coalesce(runs.location, '') end as location
    , runs.price
    -- MIT Learn kept a currency pycountry knows and fell back to USD otherwise.
    , case when runs.is_known_currency then runs.currency else 'USD' end as currency
    , runs.instructors
    , coalesce(runs.duration, '') as duration
    -- learn_duration_weeks reads an hour unit as weeks ("6 hours/day" is 6 weeks), as
    -- MIT Learn did. int__see__course_runs has the duration's own unit.
    , {{ learn_duration_weeks('runs.duration', 'min') }} as min_weeks
    , {{ learn_duration_weeks('runs.duration', 'max') }} as max_weeks
    , coalesce(runs.time_commitment, '') as time_commitment
    -- parse_resource_commitment: the first number, and the number right after the
    -- first run of non-digits, else the first again. "8 hours, 5 days/week" reads
    -- as 8 to 5 and "8 hours/day" as 8 weekly hours, as they did in MIT Learn.
    , cast({{ regexp_extract_or_null(
        "trim(runs.time_commitment)", "'^(\\d+)\\D+'", 1
    ) }} as integer) as min_weekly_hours
    , cast(coalesce(
        {{ regexp_extract_or_null("trim(runs.time_commitment)", "'^\\d+\\D+(\\d+)'", 1) }}
        , {{ regexp_extract_or_null("trim(runs.time_commitment)", "'^(\\d+)\\D+'", 1) }}
    ) as integer) as max_weekly_hours
    , 'Current' as status
    , true as published
from {{ ref('int__see__course_runs') }} as runs
inner join courses on runs.readable_id = courses.readable_id
