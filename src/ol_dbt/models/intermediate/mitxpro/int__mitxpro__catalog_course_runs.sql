-- The course runs xPRO's catalog API (/api/courses/, /api/programs/) considers: live runs
-- with a start date. is_unexpired and current_price follow CourseRun.is_unexpired and
-- CourseRun.current_price in mitxpro's courses/models.py, evaluated when this model builds.

with runs as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

-- A product belongs to one run (unique on content type and object id)
, products as (
    select * from {{ ref('int__mitxpro__catalog_product_prices') }}
    where product_is_active and courserun_id is not null
)

select
    runs.courserun_id
    , runs.course_id
    , runs.courserun_readable_id
    , runs.courserun_title
    , runs.courserun_start_on
    , runs.courserun_end_on
    , runs.courserun_enrollment_start_on
    , runs.courserun_enrollment_end_on
    , products.product_id
    , products.product_current_price as courserun_current_price
    , (
        (
            runs.courserun_end_on is null
            or {{ from_iso8601_timestamp('runs.courserun_end_on') }} > current_timestamp
        )
        and (
            runs.courserun_enrollment_end_on is null
            or {{ from_iso8601_timestamp('runs.courserun_enrollment_end_on') }} > current_timestamp
        )
        and (
            runs.courserun_enrollment_start_on is null
            or {{ from_iso8601_timestamp('runs.courserun_enrollment_start_on') }} <= current_timestamp
        )
    ) as courserun_is_unexpired
from runs
left join products on runs.courserun_id = products.courserun_id
where runs.courserun_is_live and runs.courserun_start_on is not null
