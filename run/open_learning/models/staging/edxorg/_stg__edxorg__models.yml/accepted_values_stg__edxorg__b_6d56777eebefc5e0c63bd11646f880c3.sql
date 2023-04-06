select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (




    with all_values as (

        select
            user_highest_education as value_field
            , count(*) as n_records

        from ol_data_lake_production.ol_warehouse_production_staging.stg__edxorg__bigquery__mitx_person_course
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
            , 'Doctorate in science or engineering'
            , 'Doctorate in another field'
            , ''
            , 'None'
        )




) as dbt_internal_test
