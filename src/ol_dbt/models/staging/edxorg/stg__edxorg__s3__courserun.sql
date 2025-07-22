with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__course_structure__course_metadata') }}
)

{{ deduplicate_raw_table(order_by='_airbyte_extracted_at' , partition_columns = 'course_id') }}
, cleaned as (
    select
        course_number
        , institution as courserun_institution
        , course_id as courserun_readable_id
        , replace(replace(course_id, 'course-v1:', ''), '+', '/') as courserun_edx_readable_id
        , title as courserun_title
        , semester as courserun_tag
        , self_paced as courserun_is_self_paced
        , cast(json_parse(json_query(instructor_info, 'lax $.instructors.name' with array wrapper)) as array(varchar)) --noqa
            as courserun_instructors
        , case
            when lower(course_start) = 'null' then null
            else {{ cast_timestamp_to_iso8601('course_start') }}
        end as courserun_start_on
        , case
            when lower(course_end) = 'null' then null
            else {{ cast_timestamp_to_iso8601('course_end') }}
        end as courserun_end_on
        , case
            when lower(enrollment_start) = 'null' then null
            else {{ cast_timestamp_to_iso8601('enrollment_start') }}
        end as courserun_enrollment_start_on
        , case
            when lower(enrollment_end) = 'null' then null
            else {{ cast_timestamp_to_iso8601('enrollment_end') }}
        end as courserun_enrollment_end_on
    from most_recent_source
)

select * from cleaned
