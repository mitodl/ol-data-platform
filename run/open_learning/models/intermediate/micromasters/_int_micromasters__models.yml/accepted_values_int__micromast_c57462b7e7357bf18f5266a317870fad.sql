select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            programrequirement_type as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_intermediate.int__micromasters__program_requirements
        group by programrequirement_type

    )

    select *
    from all_values
    where
        value_field not in (
            'Core', 'Elective'
        )




) as dbt_internal_test
