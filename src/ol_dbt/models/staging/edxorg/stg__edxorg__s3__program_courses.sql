with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__program_courses') }}
)

{{ deduplicate_query(cte_name1='source', cte_name2='most_recent_source'
, partition_columns = 'program_uuid, course_key') }}

, cleaned as (
    select
        program_uuid
        , {{ format_course_id('course_key') }} as course_readable_id
        , course_title
        , course_short_description as course_description
        , course_type
    from most_recent_source
)

select * from cleaned
