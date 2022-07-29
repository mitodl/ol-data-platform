-- xPro Online Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__courses_course') }}
),

cleaned as (
    select 
        -- int, sequential ID tracking a single xPro course
        id
        -- boolean, indicating whether the course is available to users
        , live
        -- str, title of the course
        , title
        -- int, unique ID pointing to the "program" which this course is part of
        , program_id
        -- str, Open edX ID formatted as course-v1:{org}+{course code}
        , readable_id
        -- int, ...
        , position_in_program
        -- timestamp, specifying when an enrollment was initially created
        , cast(created_on[1] as timestamp(6)) as created_on
        -- timestamp, specifying when an enrollment was most recently updated
        , cast(updated_on[1] as timestamp(6)) as updated_on
    from source
)

select * from cleaned
