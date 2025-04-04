with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__tables__student_courseaccessrole') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'course_id, user_id, role') }}


, cleaned as (
    select
        course_id as courserun_readable_id
        , cast(user_id as integer) as user_id
        , role as courseaccess_role
        , replace(replace(course_id, 'course-v1:', ''), '+', '/') as courserun_edx_readable_id
        , if(lower(org) = 'mitxt', 'MITxT', org) as organization
    from most_recent_source
)

select * from cleaned
