select
    count(*) as failures
    , count(*) != 0 as should_warn
    , count(*) != 0 as should_error
from (

    with a as (

        select count(*) as expression

        from
            ol_data_lake_production.ol_warehouse_production_intermediate.int__edxorg__mitx_courseruns


    )

    , b as (

        select count(*) * 1 as expression

        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__edxorg__bigquery__mitx_courserun


    )

    , final as (

        select

            a.expression
            , b.expression as compare_expression
            , abs(coalesce(a.expression, 0) - coalesce(b.expression, 0)) as expression_difference
            , abs(coalesce(a.expression, 0) - coalesce(b.expression, 0))
            / nullif(a.expression * 1.0, 0) as expression_difference_percent
        from

            a
        cross join b

    )

    -- DEBUG:
    -- select * from final
    select *
    from final
    where

        expression_difference > 0.0


) as dbt_internal_test
