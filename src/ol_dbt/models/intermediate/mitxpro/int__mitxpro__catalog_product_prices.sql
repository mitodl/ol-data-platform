-- The price xPRO's catalog API reports for a course run or program: the price of the most
-- recently created version of its product (Product.latest_version in mitxpro). One row per
-- product. Product's default manager hides inactive products, which product_is_active lets
-- a reader reproduce.

with products as (
    select * from {{ ref('int__mitxpro__ecommerce_product') }}
)

, latest_versions as (
    select
        product_id
        , productversion_price
        , row_number() over (
            partition by product_id order by productversion_created_on desc, productversion_id desc
        ) as version_rank
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_productversion') }}
)

select
    products.product_id
    , products.courserun_id
    , products.program_id
    , products.product_is_active
    , latest_versions.productversion_price as product_current_price
from products
left join latest_versions
    on products.product_id = latest_versions.product_id and latest_versions.version_rank = 1
