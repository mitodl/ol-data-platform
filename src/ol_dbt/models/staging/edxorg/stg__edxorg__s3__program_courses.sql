with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__program_course') }}
)

{{ deduplicate_raw_table(raw_table='raw__edxorg__s3__program_course', partition_columns = 'program_uuid, course_key') }}
, cleaned as (
    select
        program_uuid
        , {{ format_course_id('course_key') }} as course_readable_id
        , course_title
        , course_short_description as course_description
        , course_type
        -- the key as the programs API gives it ({org}+{course}), which is MIT Learn's
        -- readable_id for the course
        , course_key
        , course_position
        , excluded_from_search as course_is_excluded_from_search
        -- the program API's runs for this course, as a JSON array
        , course_runs as course_runs_json
        , retrieved_at as program_course_retrieved_at
    from most_recent_source
)

select * from cleaned
