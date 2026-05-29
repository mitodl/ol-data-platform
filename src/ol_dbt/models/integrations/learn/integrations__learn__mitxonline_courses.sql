{#
  integrations__learn__mitxonline_courses
  Exposes MITx Online courses for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select * from {{ ref('int__mitxonline__courses') }}
)

, raw_course_runs as (
    select
        course_id
        , concat(
            courserun_readable_id,
            '|', coalesce(cast(courserun_start_on as varchar), ''),
            '|', coalesce(cast(courserun_end_on as varchar), ''),
            '|', coalesce(cast(courserun_is_live as varchar), 'false')
        ) as run_string
    from {{ ref('int__mitxonline__course_runs') }}
)

, course_runs as (
    select
        course_id
        , {{ array_join('array_agg(run_string)', ';') }} as course_runs
    from raw_course_runs
    group by course_id
)

select
    courses.course_readable_id                              as readable_id
    , courses.course_title                                  as title
    , coalesce(
        courses.course_page_last_published_on,
        courses.course_page_first_published_on
    )                                                       as last_modified
    , 'mitxonline'                                          as etl_source
    , courses.course_description                            as description
    , courses.course_page_url_path                          as url
    , null                                                  as image_url
    , courses.course_is_live                                as published
    , 'mitxonline'                                          as platform
    , courses.course_page_slug                              as page_slug
    , courses.course_topics                                 as topics
    , courses.course_instructors                            as instructors
    , courses.course_certification_type                     as certification_type
    , courses.course_price                                  as price
    , courses.course_length                                 as length
    , courses.course_effort                                 as effort
    , course_runs.course_runs                               as runs
from courses
left join course_runs on courses.course_id = course_runs.course_id
where courses.course_is_live = true
