-- MITx Online Program Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__courses_program') }}
)

, cleaned as (
    select
        id as program_id
        , live as program_is_live
        , title as program_title
        , readable_id as program_readable_id
        , program_type
        , availability as program_availability
        , if(program_type like 'MicroMasters%', true, false) as program_is_micromasters
        , if(program_type like 'MicroMasters%', 'MicroMasters Credential', 'Certificate of Completion')
        as program_certification_type
        , if(readable_id like '%DEDP%', true, false) as program_is_dedp
        ,{{ cast_timestamp_to_iso8601('created_on') }} as program_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as program_updated_on
    from source
)

select * from cleaned
