with source as (

    select * from dev.main_raw.raw__micromasters__app__postgres__ecommerce_line

)

, renamed as (

    select
        id as line_id
        , course_key as courserun_edx_readable_id
        , price as line_price
        , description as line_description
        ,
        order_id
        ,
        to_iso8601(from_iso8601_timestamp(created_at))
        as line_created_on
        , to_iso8601(from_iso8601_timestamp(modified_at))
        as line_updated_on
    from source

)

select * from renamed
