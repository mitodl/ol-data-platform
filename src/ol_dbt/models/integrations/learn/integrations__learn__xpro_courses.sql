{#
  integrations__learn__xpro_courses
  Exposes xPRO courses for MIT Learn's Trino-pull ETL.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select * from {{ ref('int__mitxpro__courses') }}
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
    from {{ ref('int__mitxpro__course_runs') }}
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
        courses.cms_coursepage_last_published_on,
        courses.cms_coursepage_first_published_on,
        {{ cast_timestamp_to_iso8601('current_timestamp') }}
    )                                                       as last_modified
    , 'xpro'                                                as etl_source
    , courses.cms_coursepage_description                    as description
    , courses.cms_coursepage_url_path                       as url
    , null                                                  as image_url
    , courses.course_is_live                                as published
    , courses.platform_name                                 as platform
    , courses.cms_coursepage_slug                           as page_slug
    , courses.course_topics                                 as topics
    , courses.course_instructors                            as instructors
    , courses.cms_coursepage_duration                       as length
    , courses.cms_coursepage_time_commitment                as effort
    , courses.cms_coursepage_format                         as format
    , course_runs.course_runs                               as runs
from courses
left join course_runs on courses.course_id = course_runs.course_id
where courses.course_is_live = true
