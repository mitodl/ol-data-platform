select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        b2bcoupon_id as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__b2becommerce_b2bcoupon
    where b2bcoupon_id is not null
    group by b2bcoupon_id
    having count(*) > 1




) as dbt_internal_test
