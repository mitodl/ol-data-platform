with lines as (
    select * from {{ ref('stg__mitxonline__app__postgres__ecommerce_line') }}
)

, contenttypes as (
    select * from {{ ref('stg__mitxonline__app__postgres__django_contenttype') }}
)

, versions as (
    select * from {{ ref('stg__mitxonline__app__postgres__reversion_version') }}
    where contenttype_id in (
        select contenttype_id from
            contenttypes where contenttype_full_name = 'ecommerce_product'
    )
)

, intermediate_products_view as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

select
    lines.line_id
    , lines.order_id
    , lines.line_created_on
    , lines.product_version_id
    , intermediate_products_view.product_type
    , intermediate_products_view.product_id
    , intermediate_products_view.courserun_id
    , intermediate_products_view.programrun_id
from lines
inner join versions on versions.version_id = lines.product_version_id
inner join intermediate_products_view on intermediate_products_view.product_id = versions.version_object_id
