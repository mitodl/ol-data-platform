with products as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__ecommerce_product') }}
)

, contenttypes as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__django_contenttype') }}
)

, courseruns as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_courserun') }}
)

, programruns as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__courses_programrun') }}
)

, product_subquery as (
    select
        products.product_id
        , products.product_price
        , products.product_is_active
        , products.product_created_on
        , products.product_description
        , case contenttypes.contenttype_full_name
            when 'courses_courserun' then products.product_object_id
        end as courserun_id
        , case contenttypes.contenttype_full_name
            when 'courses_programrun' then products.product_object_id
        end as programrun_id
        , case contenttypes.contenttype_full_name
            when 'courses_courserun' then 'course run'
            when 'courses_programrun' then 'program run'
        end as product_type
    from products
    inner join
        contenttypes
        on products.contenttype_id = contenttypes.contenttype_id
)

select
    product_subquery.*
    , courseruns.course_id
    , programruns.program_id
from product_subquery
left join courseruns on product_subquery.courserun_id = courseruns.courserun_id
left join
    programruns
    on product_subquery.programrun_id = programruns.programrun_id
