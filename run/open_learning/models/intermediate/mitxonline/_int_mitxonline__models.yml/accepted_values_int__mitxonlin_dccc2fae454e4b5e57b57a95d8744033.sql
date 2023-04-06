select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            courseware_type as value_field
            , count(*) as n_records

        from
            ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
        group by courseware_type

    )

    select *
    from all_values
    where
        value_field not in (
            'course', 'program'
        )




) as dbt_internal_test
