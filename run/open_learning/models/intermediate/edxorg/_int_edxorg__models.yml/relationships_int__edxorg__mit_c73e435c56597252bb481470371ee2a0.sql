select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with child as (
        select micromasters_course_id as from_field
        from ol_data_lake_production.ol_warehouse_production_intermediate.int__edxorg__mitx_courseruns
        where micromasters_course_id is not null
    )

    , parent as (
        select course_id as to_field
        from ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_course
    )

    select from_field

    from child
    left join parent
        on child.from_field = parent.to_field

    where parent.to_field is null




) as dbt_internal_test
