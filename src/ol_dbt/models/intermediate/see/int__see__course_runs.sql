{#
  One row per Sloan Executive Education course offering, with the normalization MIT
  Learn's legacy Sloan ETL applied to a run (transform_run in
  learning_resources/etl/sloan.py). Dates are calendar dates in US Eastern time.
#}

with offerings as (
    select * from {{ ref('stg__see__api__course_offerings') }}
)

, courses as (
    select * from {{ ref('stg__see__api__courses') }}
)

, currencies as (
    select * from {{ ref('iso_4217_currencies') }}
)

, runs as (
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
        , {{ learn_delivery('offerings.courseoffering_delivery') }} as delivery
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
        , case
            when offerings.courseoffering_delivery = 'Online' then ''
            else coalesce(offerings.courseoffering_location, '')
        end as location
        , cast(offerings.courseoffering_price as decimal(12, 2)) as price
        -- MIT Learn kept a currency pycountry knows and fell back to USD otherwise.
        , coalesce(currencies.currency_code, 'USD') as currency
        , {{ array_filter_nonempty(
            "split(" ~ regexp_replace_all(
                "trim(coalesce(offerings.courseoffering_faculty_names_raw, ''))", "'\\s*,\\s*'", "','"
            ) ~ ", ',')"
        ) }} as instructors
        , offerings.courseoffering_continuing_ed_credits as continuing_ed_credits
        , coalesce(offerings.courseoffering_duration, '') as duration
        , {{ learn_duration_weeks('offerings.courseoffering_duration', 'min') }} as min_weeks
        , {{ learn_duration_weeks('offerings.courseoffering_duration', 'max') }} as max_weeks
        , coalesce(offerings.courseoffering_time_commitment, '') as time_commitment
        -- parse_resource_commitment: the first number, and the number right after the
        -- first run of non-digits, else the first again. "8 hours, 5 days/week" reads
        -- as 8 to 5, as it did in MIT Learn.
        , cast({{ regexp_extract_or_null(
            "trim(offerings.courseoffering_time_commitment)", "'^(\\d+)\\D+'", 1
        ) }} as integer) as min_weekly_hours
        , cast(coalesce(
            {{ regexp_extract_or_null("trim(offerings.courseoffering_time_commitment)", "'^\\d+\\D+(\\d+)'", 1) }}
            , {{ regexp_extract_or_null("trim(offerings.courseoffering_time_commitment)", "'^(\\d+)\\D+'", 1) }}
        ) as integer) as max_weekly_hours
    from offerings
    inner join courses on offerings.course_id = courses.course_id
    left join currencies on offerings.courseoffering_currency = currencies.currency_code
)

select
    readable_id
    , run_id
    , run_api_position
    , title
    , url
    , start_on
    , end_on
    , delivery
    , case when is_on_demand then 'anytime' else 'dated' end as availability
    , case when is_on_demand then 'self_paced' else 'instructor_paced' end as pace
    , is_synchronous
    , is_asynchronous
    -- Synchronous first, as parse_format lists a Blended run's formats.
    , split(concat_ws(
        ','
        , case when is_synchronous then 'synchronous' end
        , case when is_asynchronous then 'asynchronous' end
    ), ',') as format
    , location
    , price
    , currency
    , instructors
    , continuing_ed_credits
    , duration
    , min_weeks
    , max_weeks
    , time_commitment
    , min_weekly_hours
    , max_weekly_hours
from runs
