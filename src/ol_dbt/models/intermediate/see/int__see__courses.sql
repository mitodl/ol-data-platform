{#
  One row per Sloan Executive Education course, including the ones MIT Learn's legacy
  Sloan ETL skipped (no URL, or no offerings), which has_url and has_offerings mark.
  The topic is Sloan's own. MIT Learn's shaping (which courses load, its topic names,
  the first offering's credits, delivery, pace and format codes) is in
  integrations__learn__see_courses. Runs are in int__see__course_runs.
#}

with courses as (
    select * from {{ ref('stg__see__api__courses') }}
)

, offered_courses as (
    select distinct course_id
    from {{ ref('stg__see__api__course_offerings') }}
)

select
    courses.course_id as readable_id
    , courses.course_title as title
    , courses.course_description as description
    , courses.course_url as url
    , courses.course_image_url as image_url
    , courses.course_certification_type as sloan_certification_type
    , courses.course_updated_on as updated_on
    , courses.course_topic_raw as topic_raw
    -- Sloan gives one "Category: Topic" per course; the topic is the part after the
    -- last colon.
    , nullif(trim({{ regexp_extract_or_null('courses.course_topic_raw', "'([^:]*)$'", 1) }}), '') as topic
    , coalesce(courses.course_url, '') != '' as has_url
    , offered_courses.course_id is not null as has_offerings
from courses
left join offered_courses on courses.course_id = offered_courses.course_id
