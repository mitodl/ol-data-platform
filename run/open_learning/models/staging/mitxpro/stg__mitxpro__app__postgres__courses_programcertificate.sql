create table ol_data_lake_production.ol_warehouse_production_staging.stg__mitxpro__app__postgres__courses_programcertificate__dbt_tmp

as (
    -- MIT xPro Users Program Certificate Information

    with source as (
        select *
        from ol_data_lake_production.ol_warehouse_production_raw.raw__xpro__app__postgres__courses_programcertificate
    )

    , cleaned as (
        select
            id as programcertificate_id
            , uuid as programcertificate_uuid
            , program_id
            , user_id
            , certificate_page_revision_id  --- rename it after the referenced model is created
            , is_revoked as programcertificate_is_revoked
            ,
            to_iso8601(from_iso8601_timestamp(created_on))
            as programcertificate_created_on
            ,
            to_iso8601(from_iso8601_timestamp(updated_on))
            as programcertificate_updated_on
        from source
    )

    select * from cleaned
);
