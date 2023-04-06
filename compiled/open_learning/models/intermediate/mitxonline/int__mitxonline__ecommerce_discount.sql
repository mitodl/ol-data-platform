with discount as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_discount
)

select
    discount_id
    , discount_amount
    , discount_created_on
    , discount_updated_on
    , discount_code
    , discount_type
    , discount_activated_on
    , discount_expires_on
    , discount_max_redemptions
    , discount_redemption_type
    , discount_source
from discount
