with b2breceipts as (
    select *
    from dev.main_staging.stg__mitxpro__app__postgres__b2becommerce_b2breceipt
)

select
    b2breceipt_id
    , b2breceipt_created_on
    , b2breceipt_updated_on
    , b2breceipt_data
    , b2border_id
from b2breceipts
