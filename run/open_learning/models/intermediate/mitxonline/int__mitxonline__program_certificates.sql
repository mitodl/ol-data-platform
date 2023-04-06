create table ol_data_lake_production.ol_warehouse_production_intermediate.int__mitxonline__program_certificates__dbt_tmp

as (
    -- Program Certificate information for MITx Online

    with certificates as (
        select *
        from
            ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_programcertificate
    )

    , programs as (
        select *
        from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__courses_program
    )

    , users as (
        select * from ol_data_lake_production.ol_warehouse_production_staging.stg__mitxonline__app__postgres__users_user
    )

    , program_certificates as (
        select
            certificates.programcertificate_id
            , certificates.programcertificate_uuid
            , certificates.program_id
            , programs.program_title
            , programs.program_readable_id
            , certificates.programcertificate_is_revoked
            , certificates.programcertificate_created_on
            , certificates.programcertificate_updated_on
            , certificates.user_id
            , users.user_username
            , users.user_email
        from certificates
        inner join programs on certificates.program_id = programs.program_id
        inner join users on certificates.user_id = users.user_id
    )

    select * from program_certificates
);
