{#
  integrations__learn__xpro_runs
  Runs of the courses in integrations__learn__xpro_courses, as xPRO's catalog API lists them
  under a course: live, started or scheduled, not past their end or enrollment window, and
  with an active product. Runs that drop out of this set are ones MIT Learn unpublishes.
  Contract: docs/learn_marts_contract.md
#}

with runs as (
    select * from {{ ref('int__mitxpro__catalog_course_runs') }}
    where courserun_is_unexpired and product_id is not null
)

, courses as (
    select readable_id from {{ ref('integrations__learn__xpro_courses') }}
)

, course_ids as (
    select
        course_id
        , course_readable_id
    from {{ ref('int__mitxpro__courses') }}
)

, pages as (
    select
        course_id
        , page_instructors
    from {{ ref('int__mitxpro__catalog_pages') }}
    where course_id is not null
)

select
    course_ids.course_readable_id as readable_id
    , runs.courserun_readable_id as run_id
    , runs.courserun_title as title
    , runs.courserun_start_on as start_date
    , runs.courserun_end_on as end_date
    , runs.courserun_enrollment_start_on as enrollment_start
    , runs.courserun_enrollment_end_on as enrollment_end
    , runs.courserun_current_price as price
    , pages.page_instructors as instructors
    , coalesce(runs.courserun_current_price > 0, false) as published
from runs
inner join course_ids on runs.course_id = course_ids.course_id
inner join courses on course_ids.course_readable_id = courses.readable_id
left join pages on runs.course_id = pages.course_id
