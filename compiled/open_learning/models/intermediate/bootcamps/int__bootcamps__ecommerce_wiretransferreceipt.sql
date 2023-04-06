with receipts as (
    select *
    from dev.main_staging.stg__bootcamps__app__postgres__ecommerce_wiretransferreceipt
)

select
    wiretransferreceipt_id
    , wiretransferreceipt_created_on
    , wiretransferreceipt_updated_on
    , wiretransferreceipt_data
    , order_id
from receipts
