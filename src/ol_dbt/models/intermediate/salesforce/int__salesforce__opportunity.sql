with stg_opportunity as (select * from {{ ref("stg__salesforce__opportunity") }})

select
    opportunity_id,
    opportunity_name,
    opportunity_type,
    opportunity_business_type,
    opportunity_agreement_type,
    opportunity_mit_is_sales_lead,
    opportunity_revenue_share_or_commission_contract,
    opportunity_revenue_share_or_commission_amount,
    opportunity_stage,
    opportunity_amount,
    opportunity_probability,
    opportunity_is_won,
    opportunity_is_closed,
    opportunity_num_enrollment_codes_redeemed,
    opportunity_num_of_seats_purchased,
    opportunity_price_per_seat,
    opportunity_leadsource,
    opportunity_nextstep,
    opportunity_has_lineitem,
    opportunity_data_quality_score,
    opportunity_data_quality_description,
    opportunity_close_date,
    opportunity_created_on,
    opportunity_modified_on
from stg_opportunity
