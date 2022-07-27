-- xPro Course Run Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__courses_courserun') }}
),

cleaned as (
    select 
        -- int, sequential ID tracking a single course run
        id
        -- boolean, indicating whether the course is available to users
        , live
        -- str, title of the course
        , title
        -- int, ID specifying a course in the courses_course table
        , course_id
        -- str, url location for the course in xPro
        , courseware_url_path
        -- timestamp, specifying when a course run was initially created
        , cast(created_on[1] as timestamp(6)) as created_on
        -- timestamp, specifying when a course run was most recently updated
        , cast(updated_on[1] as timestamp(6)) as updated_on
    from source
)

select * from cleaned
