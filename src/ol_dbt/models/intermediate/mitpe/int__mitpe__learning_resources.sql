{#
  One row per MIT Professional Education feed item, course or program, with the
  normalization MIT Learn's legacy mitpe ETL applied (learning_resources/etl/mitpe.py).
  Runs are in int__mitpe__learning_resource_runs; program membership is in
  int__mitpe__program_courses.
#}

with source as (
    select * from {{ ref('stg__mitpe__api__courses') }}
)

, normalized as (
    select
        course_uuid as readable_id
        -- The feed labels most items, and anything labelled other than Program is a
        -- course. Unlabelled items are courses only if they grant a Certificate of
        -- Completion.
        , case
            when course_resource_type is not null and course_resource_type != ''
                then case when lower(course_resource_type) = 'program' then 'program' else 'course' end
            when '|' || course_certificates_raw || '|' like '%|Certificate of Completion|%'
                then 'course'
            else 'program'
        end as resource_type
        , {{ html_unescape(strip_whitespace('course_title')) }} as title
        , {{ url_join("'" ~ var("mitpe_url") ~ "'", 'course_url') }} as url
        , case
            when course_image_src is not null and course_image_src != ''
                then {{ url_join("'" ~ var("mitpe_url") ~ "'", 'course_image_src') }}
        end as image_url
        , course_image_alt as image_alt
        , course_description as description
        , case
            when course_topics_raw is not null and course_topics_raw != ''
                then split({{ html_unescape('course_topics_raw') }}, '|')
        end as topics
        , {{ learn_delivery('course_learning_format') }} as delivery
        , case
            when course_learning_format in ('In Person', 'On Campus', 'Blended')
                then coalesce(course_location, '')
            else ''
        end as location
        , coalesce(course_duration, '') as duration
        , {{ learn_duration_weeks('course_duration', 'min') }} as min_weeks
        , {{ learn_duration_weeks('course_duration', 'max') }} as max_weeks
        -- A price without a usable number (e.g. "Contact us") is no price rather than
        -- a failed build.
        , {{ try_cast(regexp_replace_all('course_price_raw', "'[^0-9.]'", "''"), 'decimal(12, 2)') }} as price
        -- lead instructors first, then the rest
        , {{ array_filter_nonempty(
            "split(" ~ html_unescape(
                "concat(coalesce(course_lead_instructors_raw, ''), '|', coalesce(course_instructors_raw, ''))"
            ) ~ ", '|')"
        ) }} as instructors
    from source
)

select
    readable_id
    , resource_type
    , title
    , url
    , image_url
    , image_alt
    , description
    , topics
    , delivery
    , location
    , duration
    , min_weeks
    , max_weeks
    , price
    , instructors
from normalized
