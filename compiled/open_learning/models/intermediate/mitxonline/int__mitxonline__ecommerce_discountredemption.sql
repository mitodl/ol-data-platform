with discountredemption as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_discountredemption
)


select
    discountredemption_id
    , user_id
    , discountredemption_timestamp
    , order_id
    , discount_id
from discountredemption
