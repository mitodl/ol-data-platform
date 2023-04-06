select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            programrequirement_operator as value_field
            , count(*) as n_records

        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programrequirement
        group by programrequirement_operator

    )

    select *
    from all_values
    where
        value_field not in (
            'all_of', 'min_number_of'
        )




) as dbt_internal_test
