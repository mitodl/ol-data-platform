select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with child as (
        select user_id as from_field
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__program_certificates
        where user_id is not null
    )

    , parent as (
        select user_id as to_field
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__users
    )

    select from_field

    from child
    left join parent
        on child.from_field = parent.to_field

    where parent.to_field is null




) as dbt_internal_test
