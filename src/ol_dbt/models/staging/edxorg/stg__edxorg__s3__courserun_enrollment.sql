with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__edxorg__s3__tables__student_courseenrollment') }}
)

{{ deduplicate_query(cte_name1='source', cte_name2='most_recent_source', partition_columns='id') }}

, cleaned as (
    --- all values are ingested as string, so we need to cast here to match other data sources
    select
        cast(id as integer) as courserunenrollment_id
        , course_id as courserun_readable_id
        , cast(user_id as integer) as user_id
        , cast(is_active as boolean) as courserunenrollment_is_active
        , mode as courserunenrollment_mode
        , to_iso8601(date_parse(created, '%Y-%m-%d %H:%i:%s')) as courserunenrollment_created_on
    from most_recent_source
)

select * from cleaned
