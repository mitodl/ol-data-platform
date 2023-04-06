select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (





    with grouped_expression as (
        select course_id is not null as expression





        from
            ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
        where
            courseware_type = 'course'



    )

    , validation_errors as (

        select *
        from
            grouped_expression
        where
            not(expression = true)

    )

    select *
    from validation_errors





) as dbt_internal_test
