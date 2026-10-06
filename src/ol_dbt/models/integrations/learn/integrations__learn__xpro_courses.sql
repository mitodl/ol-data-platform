{#
  integrations__learn__xpro_courses
  The courses xPRO's catalog API (/api/courses/) lists, with the fields MIT Learn's xPRO ETL
  (learning_resources/etl/xpro.py) reads from it. Runs are in integrations__learn__xpro_runs.
  Contract: docs/learn_marts_contract.md
#}

with courses as (
    select
        courses.course_id
        , courses.course_readable_id
        , courses.course_title
        , courses.course_is_live
        , platforms.platform_name
    from {{ ref('stg__mitxpro__app__postgres__courses_course') }} as courses
    left join {{ ref('stg__mitxpro__app__postgres__courses_platform') }} as platforms
        on courses.platform_id = platforms.platform_id
)

, pages as (
    select * from {{ ref('int__mitxpro__catalog_pages') }}
    where course_id is not null
)

, topics as (
    select
        course_id
        , array_agg(distinct coursetopic_name order by coursetopic_name) as topics
    from {{ ref('int__mitxpro__courses_to_topics') }}
    group by course_id
)

, priced_runs as (
    select distinct course_id
    from {{ ref('int__mitxpro__catalog_course_runs') }}
    where courserun_is_unexpired and courserun_current_price > 0
)

select
    courses.course_readable_id as readable_id
    , courses.course_title as title
    , coalesce(
        pages.page_last_published_on
        , pages.page_first_published_on
        , {{ cast_timestamp_to_iso8601('current_timestamp') }}
    ) as last_modified
    , 'xpro' as etl_source
    , pages.page_description as description
    , '{{ var("mitxpro_url") }}/courses/' || courses.course_readable_id || '/' as url
    , coalesce(pages.page_thumbnail_url, '{{ var("mitxpro_url") }}/static/images/mit-dome.png') as image_url
    , priced_runs.course_id is not null as published
    , courses.platform_name as platform
    , topics.topics
    , pages.page_format as format
    , 'dated' as availability
    , pages.page_ceus as continuing_ed_credits
    , pages.page_duration as duration
    , pages.page_min_weeks as min_weeks
    , pages.page_max_weeks as max_weeks
    , pages.page_time_commitment as time_commitment
    , pages.page_min_weekly_hours as min_weekly_hours
    , pages.page_max_weekly_hours as max_weekly_hours
from courses
inner join pages on courses.course_id = pages.course_id
left join topics on courses.course_id = topics.course_id
left join priced_runs on courses.course_id = priced_runs.course_id
-- A course with both a course page and an external course page is staged with its course
-- page only. The API would also list it on a live external page; no course has both.
where courses.course_is_live and pages.page_is_live
