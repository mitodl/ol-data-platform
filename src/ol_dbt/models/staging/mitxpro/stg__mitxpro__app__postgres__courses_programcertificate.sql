-- MIT xPro Users Program Certificate Information

with source as (
    select *
    from
        {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_programcertificate') }}
)

, cleaned as (
    select
        id as programcertificate_id
        , uuid as programcertificate_uuid
        , program_id
        , user_id
        --- rename it after the referenced model is created
        , certificate_page_revision_id
        , is_revoked as programcertificate_is_revoked
        , to_iso8601(
            from_iso8601_timestamp(created_on)
        ) as programcertificate_created_on
        , to_iso8601(
            from_iso8601_timestamp(updated_on)
        ) as programcertificate_updated_on
    from source
)

select * from cleaned
