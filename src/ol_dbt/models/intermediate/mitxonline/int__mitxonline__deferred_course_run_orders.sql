-- The course runs orders were bought for and no longer pay for. A deferral moves the paid
-- course run record to the new run and leaves the order's line on the old one, so the
-- order has a record on a run it has no line for, and none for the run on that line.
with order_runs as (
    select distinct
        order_id
        , courserun_id
    from {{ ref('int__mitxonline__ecommerce_order') }}
    where courserun_id is not null
)

, paid_course_runs as (
    select distinct
        order_id
        , courserun_id
    from {{ ref('stg__mitxonline__app__postgres__courses_paidcourserun') }}
)

-- A record on a run the order has no line for is what shows a move. On a multi-line order,
-- a line can have no record without anything having moved.
, moved_orders as (
    select distinct paid_course_runs.order_id
    from paid_course_runs
    left join order_runs
        on
            paid_course_runs.order_id = order_runs.order_id
            and paid_course_runs.courserun_id = order_runs.courserun_id
    where order_runs.order_id is null
)

select
    order_runs.order_id
    , order_runs.courserun_id
from order_runs
inner join moved_orders
    on order_runs.order_id = moved_orders.order_id
left join paid_course_runs
    on
        order_runs.order_id = paid_course_runs.order_id
        and order_runs.courserun_id = paid_course_runs.courserun_id
where paid_course_runs.order_id is null
