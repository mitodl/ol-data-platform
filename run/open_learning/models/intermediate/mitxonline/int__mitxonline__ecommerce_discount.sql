create table ol_data_lake_qa.ol_warehouse_qa_intermediate.int__mitxonline__ecommerce_discount__dbt_tmp

as (
    with discount as (
        select * from ol_data_lake_qa.ol_warehouse_qa_staging.stg__mitxonline__app__postgres__ecommerce_discount
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
        , discount_is_for_flexible_pricing
    from discount
);
