create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxpro__programs__dbt_tmp

as (
    -- Program information for MITxPro

    with courses as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_program
    )

    select
        program_id
        , program_title
        , program_is_live
        , program_readable_id
    from courses
);
