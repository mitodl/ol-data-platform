with lines as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_line') }}
)

, productversions as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_productversion') }}
)

, products as (
    select *
    from {{ ref('int__mitxpro__ecommerce_product') }}
)

, programrunlines as (
    select *
    from {{ ref('stg__mitxpro__app__postgres__ecommerce_programrunline') }}
)

select
    lines.line_id
    , lines.order_id
    , lines.productversion_id
    , lines.line_created_on
    , lines.line_updated_on
    , products.product_id
    , products.courserun_id
    , products.program_id
    , products.product_type
    , programrunlines.programrun_id
from lines
inner join productversions on lines.productversion_id = productversions.productversion_id
inner join products on productversions.product_id = products.product_id
left join programrunlines on lines.line_id = programrunlines.line_id
