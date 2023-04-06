select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            platform as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_intermediate.int__combined__courserun_enrollments
        group by platform

    )

    select *
    from all_values
    where
        value_field not in (
            'Bootcamps', 'xPro', 'MITx Online', 'MicroMasters', 'edX.org'
        )




) as dbt_internal_test
