create table ol_data_lake_production.ol_warehouse_production_intermediate.int__micromasters__programs__dbt_tmp

as (
    -- MicroMasters Program Information

    with programs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__courses_program
    )

    select
        program_id
        , program_title
        , program_description
        , program_is_live
        , program_num_required_courses
    from programs
);
