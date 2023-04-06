-- MITxPro Course Topic Information

with source as (
    select * from dev.main_raw.raw__xpro__app__postgres__courses_coursetopic
)

, cleaned as (
    select
        id as coursetopic_id
        , name as coursetopic_name
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as coursetopic_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as coursetopic_updated_on
    from source
)

select * from cleaned
