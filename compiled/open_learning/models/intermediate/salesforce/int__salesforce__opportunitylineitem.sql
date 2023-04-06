with stg_opportunitylineitem as (
    select * from dev.main_staging.stg__salesforce__opportunitylineitem
)

select
    opportunitylineitem_id
    , opportunity_id
    , opportunitylineitem_product_name
    , opportunitylineitem_description
    , opportunitylineitem_product_code
    , opportunitylineitem_list_price
    , opportunitylineitem_sales_price
    , opportunitylineitem_discount_percent
    , opportunitylineitem_quantity
    , opportunitylineitem_total_price
    , opportunitylineitem_service_date
    , opportunitylineitem_created_on
    , opportunitylineitem_modified_on
from stg_opportunitylineitem
