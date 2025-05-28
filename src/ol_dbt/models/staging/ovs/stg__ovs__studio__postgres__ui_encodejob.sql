with source as (

    select
        id
        , object_id
        , content_type_id
        , message
        , state
        , created_at
        , last_modified
        , cast(json_query(replace(message, '''', '"'), 'lax $.Output.Duration' omit quotes) as decimal(38, 4))
        as video_duration
    from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__dj_elastictranscoder_encodejob') }}

    union all

    select
        id
        , object_id
        , content_type_id
        , message
        , state
        , created_at
        , last_modified
        , cast(
            json_extract(
                cast(
                    json_extract(
                        cast(
                            json_extract(json_parse(message), '$.outputGroupDetails[0]')
                            as json
                        ), '$.outputDetails[0]'
                    ) as json
                ), '$.durationInMs'
            ) as decimal(38, 4)
        ) / 1000.0 as video_duration
    from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__ui_encodejob') }}

)

, renamed as (

    select
        id as encodejob_id
        , object_id as video_id
        , content_type_id as contenttype_id
        , video_duration
        , replace(message, '''', '"') as encodejob_message
        , case
            when state = 0 then 'Submitted'
            when state = 1 then 'Progressing'
            when state = 2 then 'Error'
            when state = 3 then 'Warning'
            when state = 4 then 'Complete'
        end as encodejob_state
        ,{{ cast_timestamp_to_iso8601('created_at') }} as encodejob_created_on
        ,{{ cast_timestamp_to_iso8601('last_modified') }} as encodejob_updated_on

    from source

)

select * from renamed
