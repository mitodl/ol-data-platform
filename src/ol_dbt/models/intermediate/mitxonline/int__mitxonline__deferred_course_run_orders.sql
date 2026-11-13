-- The course runs orders were bought for and no longer pay for. A deferral moves the paid
-- course run record to another run of the same course and leaves the order's line on the
-- old one, so the order has a record on a run it has no line for, and none for the run on
-- the line of that course.
with course_runs as (
    select
        courserun_id
        , course_id
    from {{ ref('int__mitxonline__course_runs') }}
)

, order_runs as (
    select distinct
        order_lines.order_id
        , order_lines.courserun_id
        , course_runs.course_id
    from {{ ref('int__mitxonline__ecommerce_order') }} as order_lines
    inner join course_runs
        on order_lines.courserun_id = course_runs.courserun_id
)

, paid_course_runs as (
    select distinct
        paid_course_run.order_id
        , paid_course_run.courserun_id
        , course_runs.course_id
    from {{ ref('stg__mitxonline__app__postgres__courses_paidcourserun') }} as paid_course_run
    inner join course_runs
        on paid_course_run.courserun_id = course_runs.courserun_id
)

-- A record on a run the order has no line for is what shows a move. On a multi-line order,
-- a line can have no record without anything having moved, so only the line of the moved
-- record's course lost it. A record on another course's run isn't a deferral.
, moved_records as (
    select distinct
        paid_course_runs.order_id
        , paid_course_runs.course_id
    from paid_course_runs
    left join order_runs
        on
            paid_course_runs.order_id = order_runs.order_id
            and paid_course_runs.courserun_id = order_runs.courserun_id
    where order_runs.order_id is null
)

select distinct
    order_runs.order_id
    , order_runs.courserun_id
from order_runs
inner join moved_records
    on
        order_runs.order_id = moved_records.order_id
        and order_runs.course_id = moved_records.course_id
left join paid_course_runs
    on
        order_runs.order_id = paid_course_runs.order_id
        and order_runs.courserun_id = paid_course_runs.courserun_id
where paid_course_runs.order_id is null
