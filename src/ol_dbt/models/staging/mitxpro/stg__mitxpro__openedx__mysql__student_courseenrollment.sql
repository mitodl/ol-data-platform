with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__student_courseenrollment') }}

)

, renamed as (

    select
        course_id
        , mode
        , is_active
        , id
        , created
        , user_id

    from source

)

select * from renamed
