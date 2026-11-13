{#
  One row per MIT Professional Education feed item, course or program, with the
  feed's own labels and text kept and parsed where they have a meaning of their own.
  MIT Learn's shaping (delivery codes, duration in weeks, empty strings for missing
  text, location only for on-site formats) is in the integrations__learn__mitpe_*
  views. Runs are in int__mitpe__learning_resource_runs; program membership is in
  int__mitpe__program_courses.
#}

with source as (
    select * from {{ ref('stg__mitpe__api__courses') }}
)

, duration_text as (
    select
        course_uuid
        -- "7 half days" and "5 Half-Days" are one unit, so join them before taking the
        -- word after the number
        , replace(replace(lower(trim(course_duration)), 'half days', 'half-days'), 'half day', 'half-day')
            as normalized
    from source
)

, duration_parts as (
    select
        course_uuid
        , {{ regexp_extract_or_null('normalized', "'^(\\d+(?:\\.\\d+)?)'", 1) }} as min_value
        , {{ regexp_extract_or_null('normalized', "'^\\d+(?:\\.\\d+)?\\s*(?:-|–|to)\\s*(\\d+(?:\\.\\d+)?)'", 1) }}
            as max_value
        -- the first word after the number or range (and an optional hyphen, "8-week"),
        -- up to a space or punctuation ("3 mes/curso", "3 days, online",
        -- "2 days in person+3 live webinars")
        , {{ regexp_extract_or_null(
            'normalized',
            "'^\\d+(?:\\.\\d+)?(?:\\s*(?:-|–|to)\\s*\\d+(?:\\.\\d+)?)?\\s*-?\\s*([^\\s/+,.;:()]+)'",
            1
        ) }} as unit_word
    from duration_text
)

, normalized as (
    select
        source.course_uuid as readable_id
        , nullif(source.course_resource_type, '') as source_resource_type
        -- The feed labels most items, and anything labelled other than Program is a
        -- course. Unlabelled items are courses only if they grant a Certificate of
        -- Completion, a rule MIT Learn's ETL used and kept here because the feed has
        -- no better signal; is_resource_type_inferred marks those rows.
        , case
            when source.course_resource_type is not null and source.course_resource_type != ''
                then case when lower(source.course_resource_type) = 'program' then 'program' else 'course' end
            when {{ dbt.concat(["'|'", "source.course_certificates_raw", "'|'"]) }} like '%|Certificate of Completion|%'
                then 'course'
            else 'program'
        end as resource_type
        , coalesce(source.course_resource_type, '') = '' as is_resource_type_inferred
        , {{ html_unescape(strip_whitespace('source.course_title')) }} as title
        , {{ url_join("'" ~ var("mitpe_url") ~ "'", 'source.course_url') }} as url
        , case
            when source.course_image_src is not null and source.course_image_src != ''
                then {{ url_join("'" ~ var("mitpe_url") ~ "'", 'source.course_image_src') }}
        end as image_url
        , source.course_image_alt as image_alt
        , source.course_description as description
        , case
            when source.course_topics_raw is not null and source.course_topics_raw != ''
                then split({{ html_unescape('source.course_topics_raw') }}, '|')
        end as topics
        , nullif(source.course_learning_format, '') as learning_format
        , nullif(source.course_location, '') as location
        , nullif(source.course_duration, '') as duration
        , {{ try_cast('duration_parts.min_value', 'decimal(8, 2)') }} as duration_min
        , {{ try_cast('coalesce(duration_parts.max_value, duration_parts.min_value)', 'decimal(8, 2)') }}
            as duration_max
        , case
            when duration_parts.min_value is null then null
            when duration_parts.unit_word in ('hour', 'hours', 'hora', 'horas') then 'hour'
            when duration_parts.unit_word in ('half-day', 'half-days') then 'half_day'
            when duration_parts.unit_word in ('day', 'days', 'día', 'días', 'jour', 'jours') then 'day'
            when duration_parts.unit_word in (
                'week', 'weeks', 'semana', 'semanas', 'semaine', 'semaines', 'settimana', 'settimane'
            ) then 'week'
            when duration_parts.unit_word in ('month', 'months', 'mes', 'meses', 'mois', 'mesi') then 'month'
            when duration_parts.unit_word in ('semester', 'semesters', 'semestre', 'semestres') then 'semester'
        end as duration_unit
        , nullif(source.course_price_raw, '') as price_text
        -- The one amount in the text, e.g. "$3,600", "USD 3600" or "$2,500 per person".
        -- Text with no number ("Contact us") or more than one (a range) has no single
        -- price and is null, rather than its digits run together.
        , {{ try_cast(
            regexp_replace_all(
                regexp_extract_or_null('source.course_price_raw', "'^[^0-9]*(\\d[\\d,]*(?:\\.\\d+)?)[^0-9]*$'", 1)
                , "','"
                , "''"
            )
            , 'decimal(12, 2)'
        ) }} as price
        -- lead instructors first, then the rest
        , {{ array_filter_nonempty(
            "split(" ~ html_unescape(
                "concat(coalesce(source.course_lead_instructors_raw, ''), '|', coalesce(source.course_instructors_raw, ''))"
            ) ~ ", '|')"
        ) }} as instructors
    from source
    inner join duration_parts on source.course_uuid = duration_parts.course_uuid
)

select
    readable_id
    , resource_type
    , source_resource_type
    , is_resource_type_inferred
    , title
    , url
    , image_url
    , image_alt
    , description
    , topics
    , learning_format
    , location
    , duration
    , duration_min
    , duration_max
    , duration_unit
    , price_text
    , price
    , instructors
from normalized
