with discountproduct as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_discountproduct
)

select
    discountproduct_id
    , discountproduct_created_on
    , discountproduct_updated_on
    , product_id
    , discount_id
from discountproduct
