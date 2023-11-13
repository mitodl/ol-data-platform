with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__ecommerce_line') }}

)

, renamed as (

    select
        id as line_id
        , course_key as courserun_readable_id
        , replace(replace(course_key, 'course-v1:', ''), '+', '/') as courserun_edxorg_readable_id
        , cast(price as decimal(38, 2)) as line_price
        , description as line_description
        ,{{ cast_timestamp_to_iso8601('created_at') }} as line_created_on
        ,{{ cast_timestamp_to_iso8601('modified_at') }} as line_updated_on
        , order_id
    from source

)

select * from renamed
