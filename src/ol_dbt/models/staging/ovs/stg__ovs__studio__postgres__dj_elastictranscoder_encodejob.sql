with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__ovs__postgres__dj_elastictranscoder_encodejob') }}

)

, renamed as (

    select
        id as encodejob_id
        , object_id as video_id
        , content_type_id as contenttype_id
        , replace(message, '''', '"') as encodejob_message
        , json_query(replace(message, '''', '"'), 'lax $.Output.Duration' omit quotes) as video_duration
        , case
            when status = 0 then 'Submitted'
            when status = 1 then 'Progressing'
            when status = 2 then 'Error'
            when status = 3 then 'Warning'
            when status = 4 then 'Complete'
        end as encodejob_status
        ,{{ cast_timestamp_to_iso8601('created_on') }} as encodejob_created_on
        ,{{ cast_timestamp_to_iso8601('last_modified') }} as encodejob_updated_on

    from source

)

select * from renamed
