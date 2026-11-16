--MITxPro Platform Information

with source as (
    {{ live_raw_rows('raw__xpro__app__postgres__courses_platform') }}
)

, cleaned as (
    select
        id as platform_id
        , name as platform_name
    from source
)

select * from cleaned
