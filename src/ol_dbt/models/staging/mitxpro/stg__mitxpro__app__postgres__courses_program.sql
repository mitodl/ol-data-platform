-- MITxPro Program Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_program') }}
)

, cleaned as (
    select
        id as program_id
        , live as program_is_live
        , title as program_title
        , readable_id as program_readable_id
        , substring(
            readable_id, ((position('+' in readable_id)) + 1)
            , (
                position(
                    'x' in substring(
                        readable_id, position('+' in readable_id)
                    )
                ) - 2
            )
        ) as short_program_code
        , platform_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as program_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as program_updated_on
        , is_external as program_is_external

    from source
)

select * from cleaned
