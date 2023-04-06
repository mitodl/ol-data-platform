-- Bootcamps Users Course Certificate Information

with source as (
    select * from dev.main_raw.raw__bootcamps__app__postgres__klasses_bootcampruncertificate
)

, cleaned as (
    select
        id as courseruncertificate_id
        , uuid as courseruncertificate_uuid
        , bootcamp_run_id as courserun_id
        , user_id
        , is_revoked as courseruncertificate_is_revoked
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as courseruncertificate_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as courseruncertificate_updated_on
    from source
)

select * from cleaned
