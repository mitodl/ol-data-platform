-- The course runs orders were bought for and no longer pay for. A deferral moves the paid
-- course run record to the new run and leaves the order's line on the old one, so the
-- order has a record, but none for the run on its line.
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

, paid_orders as (
    select distinct order_id
    from paid_course_runs
)

select
    order_runs.order_id
    , order_runs.courserun_id
from order_runs
inner join paid_orders
    on order_runs.order_id = paid_orders.order_id
left join paid_course_runs
    on
        order_runs.order_id = paid_course_runs.order_id
        and order_runs.courserun_id = paid_course_runs.courserun_id
where paid_course_runs.order_id is null
