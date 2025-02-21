with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__s3__mitx_course_run') }}
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
        , languages as courserun_languages
        , min_effort as courserun_min_weekly_hours
        , max_effort as courserun_max_weekly_hours
        , estimated_hours as courserun_estimated_hours
        , enrollment_type as courserun_enrollment_mode
        , availability as courserun_availability
        , status as courserun_status
        , is_enrollable as courserun_is_enrollable
        , pacing_type as courserun_pace
        , if(staff = '[]', null, staff) as courserun_instructors
        , if(seats = '[]', null, seats) as courserun_enrollment_modes
        , json_query(image, 'lax $.url' omit quotes) as courserun_image_url
        , if(status = 'published' and is_enrollable, true, false) as courserun_is_published
        , case
            when pacing_type = 'self_paced' then true
            when pacing_type = 'instructor_paced' then false
        end as courserun_is_self_paced
        , case
            when weeks_to_complete = 1 then cast(weeks_to_complete as varchar) || ' week'
            when weeks_to_complete > 1 then cast(weeks_to_complete as varchar) || ' weeks'
        end as courserun_duration
        , case
            when min_effort is not null and max_effort is not null
                then cast(min_effort as varchar) || '-' || cast(max_effort as varchar) || ' hours per week'
            when min_effort is not null and max_effort is null
                then cast(min_effort as varchar) || ' hours per week'
            when min_effort is null and max_effort is not null
                then cast(max_effort as varchar) || ' hours per week'
        end as courserun_time_commitment
        , {{ cast_timestamp_to_iso8601('start_on') }} as courserun_start_on
        , {{ cast_timestamp_to_iso8601('end_on') }} as courserun_end_on
        , {{ cast_timestamp_to_iso8601('enrollment_start') }} as courserun_enrollment_start_on
        , {{ cast_timestamp_to_iso8601('enrollment_end') }} as courserun_enrollment_end_on
        , {{ cast_timestamp_to_iso8601('announcement') }} as courserun_announce_on
        , {{ cast_timestamp_to_iso8601('modified') }} as courserun_updated_at

    from most_recent_source
)

select * from cleaned
where lower(courserun_title) not like 'delete%'
