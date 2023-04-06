with source as (

    select * from dev.main_raw.raw__bootcamps__app__postgres__ecommerce_line

)

, renamed as (
    select
        id as line_id
        , price as line_price
        , description as line_description
        , order_id
        , bootcamp_run_id as courserun_id
        ,
        to_iso8601(from_iso8601_timestamp(created_on))
        as line_created_on
        ,
        to_iso8601(from_iso8601_timestamp(updated_on))
        as line_updated_on
    from source

)

select * from renamed
