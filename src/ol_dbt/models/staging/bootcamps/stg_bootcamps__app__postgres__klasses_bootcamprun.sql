-- Bootcamps Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__bootcamps__app__postgres__klasses_bootcamprun') }}
),

renamed as (
    select
        id
        , title
        , source
        , end_date
        , start_date
        , bootcamp_id
        , bootcamp_run_id
        , novoed_course_stub
    from source
)

select * from renamed
