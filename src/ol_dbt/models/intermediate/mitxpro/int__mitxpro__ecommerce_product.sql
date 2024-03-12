with products as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_product') }}
)

, contenttypes as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__django_contenttype') }}
)

, courseruns as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__courses_courserun') }}
)

, programs as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__courses_program') }}
)

, product_subquery as (
    select
        products.product_id
        , products.product_is_active
        , products.product_created_on
        , products.product_updated_on
        , products.product_is_private
        , case contenttypes.contenttype_full_name
            when 'courses_courserun' then products.product_object_id
        end as courserun_id
        , case contenttypes.contenttype_full_name
            when 'courses_program' then products.product_object_id
        end as program_id
        , case contenttypes.contenttype_full_name
            when 'courses_courserun' then 'course run'
            when 'courses_program' then 'program'
            else contenttypes.contenttype_full_name
        end as product_type
    from products
    inner join contenttypes on products.contenttype_id = contenttypes.contenttype_id
)

select
    product_subquery.*
    , courseruns.course_id
    , courseruns.courserun_readable_id
    , programs.program_readable_id
from product_subquery
left join courseruns on product_subquery.courserun_id = courseruns.courserun_id
left join programs on product_subquery.program_id = programs.program_id
