select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            programrequirement_node_type as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__programrequirements
        group by programrequirement_node_type

    )

    select *
    from all_values
    where
        value_field not in (
            'program_root', 'course', 'operator'
        )




) as dbt_internal_test
