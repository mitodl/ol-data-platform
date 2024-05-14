with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__tables__student_courseaccessrole') }}
)

{{ deduplicate_query (
   cte_name1='source',
   cte_name2='most_recent_source' ,
   partition_columns = 'course_id, user_id, role')
}}


, cleaned as (
    select
        course_id as courserun_readable_id
        , cast(user_id as integer) as user_id
        , role as courseaccess_role
        , if(lower(org) = 'mitxt', 'MITxT', org) as organization
    from most_recent_source
)

select * from cleaned
