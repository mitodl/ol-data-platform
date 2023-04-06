select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            user_highest_education as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__bootcamps__app__postgres__profiles_profile
        group by user_highest_education

    )

    select *
    from all_values
    where
        value_field not in (
            'Doctorate'
            , 'Master''s or professional degree'
            , 'Bachelor''s degree'
            , 'Associate degree'
            , 'Secondary/high school'
            , 'Junior secondary/junior high/middle school'
            , 'Elementary/primary school'
            , 'No formal education'
            , 'Other education'
            , ''
        )




) as dbt_internal_test
