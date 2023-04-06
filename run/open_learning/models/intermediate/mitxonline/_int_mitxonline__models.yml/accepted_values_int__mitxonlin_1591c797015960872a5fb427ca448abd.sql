select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            courserunenrollment_enrollment_status as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__courserunenrollments
        group by courserunenrollment_enrollment_status

    )

    select *
    from all_values
    where
        value_field not in (
            'deferred', 'transferred', 'refunded', 'enrolled', 'unenrolled', ''
        )




) as dbt_internal_test
