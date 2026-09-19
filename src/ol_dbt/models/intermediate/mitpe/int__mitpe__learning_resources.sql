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
        , {{ html_unescape(regexp_replace_all('course_title', "'^\\s+|\\s+$'", "''")) }} as title
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
        , case course_learning_format
            when 'Blended' then 'hybrid'
            when 'In Person' then 'in_person'
            when 'On Campus' then 'in_person'
            when 'Hybrid' then 'hybrid'
            when 'In person' then 'in_person'
            when 'Offline' then 'offline'
            else 'online'
        end as delivery
        , case
            when course_learning_format in ('In Person', 'On Campus', 'Blended')
                then course_location
            else ''
        end as location
        , coalesce(course_duration, '') as duration
        , {{ regexp_extract_or_null("lower(trim(course_duration))", "'^(\\d+)'", 1) }}
            as duration_min_raw
        -- A second number only counts after a separator, so "12 weeks" is not read as
        -- the range 1-2.
        , {{ regexp_extract_or_null(
            "lower(trim(course_duration))", "'^\\d+(?:\\s*(?:to|-)+\\s*|\\s+)(\\d+)'", 1
        ) }} as duration_max_raw
        -- The first English unit anywhere in the string, else the first Spanish, French
        -- or Italian one. Only whether it is a day or month unit matters below. As in
        -- MIT Learn's pattern, only the last alternative of each needs a separator
        -- after it, so "mes" doesn't match inside "semesters".
        , {{ regexp_extract_or_null(
            "lower(course_duration)",
            "'half-days|half-day|hours|hour|days|day|weeks|week|months|month(\\s|/|$)'"
        ) }} as duration_english_unit
        , {{ regexp_extract_or_null(
            "lower(course_duration)",
            "'horas|hora|días|jours|día|jour|semanas|semaines|settimanes|semana|semaine|settimane|meses|mois|mesi|mes(\\s|/|$)'"
        ) }} as duration_other_unit
        -- A price without a usable number (e.g. "Contact us") is no price rather than
        -- a failed build.
        , try_cast({{ regexp_replace_all('course_price_raw', "'[^0-9.]'", "''") }} as decimal(12, 2)) as price
        -- lead instructors first, then the rest
        , {{ array_filter_nonempty(
            "split(" ~ html_unescape(
                "concat(coalesce(course_lead_instructors_raw, ''), '|', coalesce(course_instructors_raw, ''))"
            ) ~ ", '|')"
        ) }} as instructors
    from source
)

, with_duration_unit as (
    select
        *
        , case
            when duration_english_unit like '%day%' then 'day'
            when duration_english_unit like '%month%' then 'month'
            when duration_english_unit is not null then 'week'
            when duration_other_unit in ('días', 'jours', 'día', 'jour') then 'day'
            -- "mes" can match with its trailing separator ("mes/"); MIT Learn raised
            -- KeyError on that, and it is a month
            when trim(replace(duration_other_unit, '/', '')) in ('meses', 'mois', 'mesi', 'mes') then 'month'
            when duration_other_unit is not null then 'week'
        end as duration_unit
    from normalized
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
    -- Days count as working days (5 per week, at least 1 week) and months as 4 weeks.
    -- Hour units fall through as weeks, as they did in MIT Learn.
    , case
        when duration_min_raw is null or duration_unit is null then null
        when duration_unit = 'day'
            then greatest(cast(ceil(cast(duration_min_raw as double) / 5) as integer), 1)
        when duration_unit = 'month' then cast(duration_min_raw as integer) * 4
        else cast(duration_min_raw as integer)
    end as min_weeks
    , case
        when duration_min_raw is null or duration_unit is null then null
        when duration_unit = 'day'
            then greatest(
                cast(ceil(cast(coalesce(duration_max_raw, duration_min_raw) as double) / 5) as integer), 1
            )
        when duration_unit = 'month' then cast(coalesce(duration_max_raw, duration_min_raw) as integer) * 4
        else cast(coalesce(duration_max_raw, duration_min_raw) as integer)
    end as max_weeks
    , price
    , instructors
from with_duration_unit
