with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__openedx__mysql__student_courseaccessrole') }}
)

, cleaned as (
    select
        id as courseaccessrole_id
        , course_id as courserun_readable_id
        , user_id as openedx_user_id
        , role as courseaccess_role
        , org as organization
    from source
)

select * from cleaned
