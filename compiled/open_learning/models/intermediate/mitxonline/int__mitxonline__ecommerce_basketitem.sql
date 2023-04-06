with source as (
    select *
    from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_basketitem
)

select
    basketitem_id
    , basketitem_quantity
    , basket_id
    , basketitem_created_on
    , product_id
    , basketitem_updated_on
from source
