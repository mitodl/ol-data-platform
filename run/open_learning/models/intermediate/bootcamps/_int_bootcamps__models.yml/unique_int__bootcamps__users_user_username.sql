select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    select
        user_username as unique_field
        , count(*) as n_records

    from ol_data_lake_production.ol_warehouse_production_intermediate.int__bootcamps__users
    where user_username is not null
    group by user_username
    having count(*) > 1




) as dbt_internal_test
