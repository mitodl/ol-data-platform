with userdiscount as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__ecommerce_userdiscount
)

select
    userdiscount_id
    , userdiscount_created_on
    , userdiscount_updated_on
    , user_id
    , discount_id
from userdiscount
