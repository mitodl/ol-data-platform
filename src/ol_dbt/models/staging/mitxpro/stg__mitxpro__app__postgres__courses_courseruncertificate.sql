-- MIT xPro Users Course Certificate Information

with source as (
    select *
    from
        {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_courseruncertificate') }}
)

, cleaned as (
    select
        id as courseruncertificate_id
        , uuid as courseruncertificate_uuid
        , course_run_id as courserun_id
        , user_id
        --- rename it after the referenced model is created
        , certificate_page_revision_id
        , is_revoked as courseruncertificate_is_revoked
        , to_iso8601(
            from_iso8601_timestamp(created_on)
        ) as courseruncertificate_created_on
        , to_iso8601(
            from_iso8601_timestamp(updated_on)
        ) as courseruncertificate_updated_on
    from source
)

select * from cleaned
