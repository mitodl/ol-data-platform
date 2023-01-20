-- MITx Online Users Program Certificate Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_programcertificate') }}
)

, cleaned as (
    select
        id as programcertificate_id
        , uuid as programcertificate_uuid
        , program_id
        , user_id
        , certificate_page_revision_id  --- rename it after the referenced model is created
        , is_revoked as programcertificate_is_revoked
        , {{ cast_timestamp_to_iso8601('created_on') }} as programcertificate_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as programcertificate_updated_on
    from source
)

select * from cleaned
