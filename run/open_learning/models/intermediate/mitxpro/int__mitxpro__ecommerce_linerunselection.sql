create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__ecommerce_linerunselection__dbt_tmp

as (
    with linerunselection as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__ecommerce_linerunselection
    )

    select
        linerunselection_id
        , line_id
        , courserun_id
        , linerunselection_created_on
        , linerunselection_updated_on
    from linerunselection
);
