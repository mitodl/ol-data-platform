-- Contains user profile, enrollments, grades and certificates Information for edx.org


with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__irx__edxorg__bigquery__mitx_person_course') }}

)

, renamed as (

    select
        user_id
        , course_id as courserun_readable_id
        ,{{ translate_course_id_to_platform('course_id') }} as courserun_platform

        --- users
        , username as user_username
        , yob as user_birth_year
        , profile_country as user_profile_country

        , viewed as courseactivitiy_visited_once
        , explored as courseactivitiy_viewed_half
        -- user course activities from tracking logs
        , nevents as courseactivitiy_num_events
        , ndays_act as courseactivitiy_num_activity_days
        , nprogcheck as courseactivitiy_num_progress_check
        , nproblem_check as courseactivitiy_num_problem_check
        , nshow_answer as courseactivitiy_num_show_answer
        , ntranscript as courseactivitiy_num_show_transcript
        , nseq_goto as courseactivitiy_num_seq_goto
        , nplay_video as courseactivitiy_num_play_video
        , nseek_video as courseactivitiy_num_seek_video
        , npause_video as courseactivitiy_num_pause_video
        , nvideo as courseactivitiy_num_video_interactions
        , nvideos_unique_viewed as courseactivitiy_num_unique_videos_viewed
        , nvideos_total_watched as courseactivitiy_percentage_total_videos_watched
        , avg_dt as courseactivitiy_average_time_diff_in_sec
        , sdv_dt as courseactivitiy_standard_deviation_in_sec
        , max_dt as courseactivitiy_max_diff_in_sec
        , n_dt as courseactivitiy_num_consecutive_events_used
        , sum_dt as courseactivitiy_total_elapsed_time_in_sec

        --- enrollments, grades and certificates
        , mode as courserunenrollment_enrollment_mode
        , passing_grade as courserungrade_passing_grade
        , grade as courserungrade_user_grade
        , completed as courserungrade_is_passing
        , certified as courseruncertificate_is_earned
        , cert_status as courseruncertificate_status
        , coalesce(is_active = 1, false) as courserunenrollment_is_active
        --- trino doesn't have function to convert first letter to upper case
        , regexp_replace(
            {{ transform_gender_value('gender') }}, '(^[a-z])(.)', x -> upper(x[1]) || x[2] -- noqa
        ) as user_gender
        ,{{ transform_education_value('loe') }} as user_highest_education
        ,{{ cast_timestamp_to_iso8601('start_time') }} as courserunenrollment_created_on
        ,{{ cast_timestamp_to_iso8601('verified_enroll_time') }} as courserunenrollment_enrolled_on
        ,{{ cast_timestamp_to_iso8601('verified_unenroll_time') }} as courserunenrollment_unenrolled_on
        ,{{ cast_timestamp_to_iso8601('cert_created_date') }} as courseruncertificate_created_on
        ,{{ cast_timestamp_to_iso8601('cert_modified_date') }} as courseruncertificate_updated_on
        ,{{ cast_timestamp_to_iso8601('first_event') }} as courseactivitiy_first_event_timestamp
        ,{{ cast_timestamp_to_iso8601('last_event') }} as courseactivitiy_last_event_timestamp
    from source
    where user_id is not null --- temporary fix to filter the bad data

)

select * from renamed
