select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with child as (
        select courserun_id as from_field
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__courses_courseruncertificate
        where courserun_id is not null
    )

    , parent as (
        select courserun_id as to_field
        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__courses_courserun
    )

    select from_field

    from child
    left join parent
        on child.from_field = parent.to_field

    where parent.to_field is null




) as dbt_internal_test
