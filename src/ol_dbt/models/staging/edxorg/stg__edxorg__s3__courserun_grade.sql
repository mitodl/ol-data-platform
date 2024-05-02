with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__tables__grades_persistentcoursegrade') }}
)

{{ deduplicate_query(cte_name1='source', cte_name2='most_recent_source' , partition_columns = 'course_id, user_id') }}

, cleaned as (
    select
        ---all values are ingested as string, so we need to cast here to match other data sources
        course_id as courserun_readable_id
        , cast(user_id as integer) as user_id
        , cast(percent_grade as decimal(38, 2)) as courserungrade_grade
        , letter_grade as courserungrade_letter_grade
        , grading_policy_hash as courserungrade_grading_policy_hash
        , case
            when passed_timestamp = 'NULL' then null
            else to_iso8601(from_iso8601_timestamp_nanos(
                regexp_replace(passed_timestamp, '(\d{4}-\d{2}-\d{2})[ ](\d{2}:\d{2}:\d{2})(.*?)', '$1T$2$3')
            ))
        end as courserungrade_first_passed_on
        --- use regex here to preserve the faction of second if value uses nanosecond
        , to_iso8601(from_iso8601_timestamp_nanos(
            regexp_replace(created, '(\d{4}-\d{2}-\d{2})[ ](\d{2}:\d{2}:\d{2})(.*?)', '$1T$2$3')
        )) as courserungrade_created_on
        , to_iso8601(from_iso8601_timestamp_nanos(
            regexp_replace(modified, '(\d{4}-\d{2}-\d{2})[ ](\d{2}:\d{2}:\d{2})(.*?)', '$1T$2$3')
        )) as courserungrade_updated_on
    from most_recent_source
)

select * from cleaned
