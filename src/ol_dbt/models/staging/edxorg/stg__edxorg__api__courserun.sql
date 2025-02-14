with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__mitx_course_runs') }}
)

{{ deduplicate_raw_table(order_by='retrieved_at' , partition_columns = 'run_key') }}

, cleaned as (
    select
        run_key as courserun_readable_id
        , course_key as course_readable_id
        , title as courserun_title
        , short_description as courserun_short_description
        , full_description as courserun_full_description
        , level_type as courserun_level
        , marketing_url as courserun_marketing_url
        , staff as courserun_instructors
        , languages as courserun_languages
        , min_effort as courserun_min_weekly_hours
        , max_effort as courserun_max_weekly_hours
        , estimated_hours as courserun_estimated_hours
        , pacing_type as courserun_pace
        , enrollment_type as courserun_enrollment_mode
        , availability as courserun_availability
        , status as courserun_status
        , if(seats = '[]', null, seats) as courserun_enrollment_modes
        , json_query(image, 'lax $.url' omit quotes) as courserun_image_url
        , case
            when weeks_to_complete = 1 then cast(weeks_to_complete as varchar) || ' week'
            when weeks_to_complete > 1 then cast(weeks_to_complete as varchar) || ' weeks'
        end as courserun_duration
        , {{ cast_timestamp_to_iso8601('start_on') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('end_on') }} as courserun_end_on
        , {{ cast_timestamp_to_iso8601('enrollment_start') }} as courserun_enrollment_start_on
        , {{ cast_timestamp_to_iso8601('enrollment_end') }} as courserun_enrollment_end_on
        , {{ cast_timestamp_to_iso8601('announcement') }} as courserun_announce_on
        , {{ cast_timestamp_to_iso8601('modified') }} as courserun_updated_at

    from most_recent_source
)

select * from cleaned
