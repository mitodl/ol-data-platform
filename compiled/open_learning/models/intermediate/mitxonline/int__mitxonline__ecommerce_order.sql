with lines as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_line
)

, contenttypes as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__django_contenttype
)

, versions as (
    select *
    from dev.main_staging.stg__mitxonline__app__postgres__reversion_version
    where
        contenttype_id in (
            select contenttype_id
            from
                contenttypes
            where contenttype_full_name = 'ecommerce_product'
        )
)

, orders as (
    select *
    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_order
)

, users as (
    select *
    from dev.main_staging.stg__mitxonline__app__postgres__users_user
)

, intermediate_products_view as (
    select * from dev.main_intermediate.int__mitxonline__ecommerce_product
)

select
    orders.order_id
    , orders.order_state
    , orders.order_created_on
    , orders.order_reference_number
    , orders.order_total_price_paid
    , users.user_id
    , users.user_username
    , users.user_full_name
    , users.user_email
    , lines.line_id
    , lines.product_version_id
    , intermediate_products_view.product_type
    , intermediate_products_view.product_id
    , intermediate_products_view.courserun_id
    , intermediate_products_view.programrun_id
from lines
inner join orders on orders.order_id = lines.order_id
inner join users on orders.order_purchaser_user_id = users.user_id
inner join versions on versions.version_id = lines.product_version_id
inner join intermediate_products_view on intermediate_products_view.product_id = versions.version_object_id
