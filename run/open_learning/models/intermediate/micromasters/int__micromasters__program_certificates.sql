create table ol_data_lake_production.ol_warehouse_production_intermediate.int__micromasters__program_certificates__dbt_tmp

as (
    with program_certificates as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__micromasters__app__postgres__grades_programcertificate
    )

    select
        programcertificate_id
        , programcertificate_created_on
        , programcertificate_updated_on
        , programcertificate_hash
        , program_id
        , user_id
    from program_certificates
);
