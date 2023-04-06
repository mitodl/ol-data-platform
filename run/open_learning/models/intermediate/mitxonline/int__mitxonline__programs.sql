create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__programs__dbt_tmp

as (
    -- Program information for MITx Online

    with programs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_program
    )

    select
        program_id
        , program_title
        , program_is_live
        , program_readable_id
    from programs
);
