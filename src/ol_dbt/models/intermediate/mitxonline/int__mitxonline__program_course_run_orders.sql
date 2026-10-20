-- A program buyer's course run orders use the program's internal discount and do not
-- store the program order, so match it by learner and program.
with orders as (
    select * from {{ ref('int__mitxonline__ecommerce_order') }}
)

, discountproducts as (
    select * from {{ ref('stg__mitxonline__app__postgres__ecommerce_discountproduct') }}
)

, products as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

-- The learner can buy a parent program (UAI+B2C) and then enroll through a
-- sub-program (UAI+B2C.2), whose internal discount the course run order then uses.
, parent_programs as (
    select distinct
        required_program_id as program_id
        , program_id as parent_program_id
    from {{ ref('stg__mitxonline__app__postgres__courses_programrequirement') }}
    where required_program_id is not null
)

, course_run_orders as (
    select
        orders.line_id
        , orders.order_id
        , orders.user_id
        , orders.order_created_on
        , products.program_id
    from orders
    inner join discountproducts on orders.discount_id = discountproducts.discount_id
    inner join products on discountproducts.product_id = products.product_id
    where
        orders.discount_redemption_type = 'internal'
        and orders.product_type = 'course run'
        and products.product_type = 'program'
)

, program_orders as (
    select
        order_id
        , user_id
        , program_id
        , order_created_on
    from orders
    where
        product_type = 'program'
        and order_state in ('fulfilled', 'refunded')
)

, matches as (
    select
        course_run_orders.line_id
        , course_run_orders.order_id
        , program_orders.order_id as program_order_id
        , row_number() over (
            partition by course_run_orders.line_id
            order by
                case when program_orders.program_id = course_run_orders.program_id then 0 else 1 end
                , program_orders.order_created_on desc
        ) as row_num
    from course_run_orders
    left join parent_programs on course_run_orders.program_id = parent_programs.program_id
    inner join program_orders
        on
            course_run_orders.user_id = program_orders.user_id
            and program_orders.order_created_on <= course_run_orders.order_created_on
            and program_orders.program_id in (course_run_orders.program_id, parent_programs.parent_program_id)
)

select
    line_id
    , order_id
    , program_order_id
from matches
where row_num = 1
