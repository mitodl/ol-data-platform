with source as (
    select * from dev.main_raw.raw__mitx__openedx__mysql__student_courseaccessrole
)

, cleaned as (
    select
        id as courseaccessrole_id
        , course_id as courserun_readable_id
        , user_id
        , role as courseaccessrole_role
        , case
            when lower(org) = 'mitx' then 'MITx'
            else org
        end as courserun_org
    from source
)

select * from cleaned
