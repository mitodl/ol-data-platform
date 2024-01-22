-- It contains user profiles, enrollments and certificate detail of learners in a given course
-- There are some fields overlapping with person_course

with source as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__irx__edxorg__bigquery__mitx_user_info_combo') }}
)

, cleaned as (

    select
        user_id
        ,{{ translate_course_id_to_platform('enrollment_course_id') }} as courserun_platform
        , email as user_email
        , username as user_username
        , id_map_hash_id as user_map_hash_id
        , profile_name as user_full_name
        , profile_mailing_address as user_mailing_address
        , profile_goals as user_profile_goals
        , profile_city as user_city
        -- filter outcountry values which are not valid country codes which consist of
        -- two capital letters
        , case when regexp_like(profile_country, '^[A-Z]{2}$') then profile_country end as user_country
        , profile_year_of_birth as user_birth_year
        , profile_meta as user_profile_meta
        , enrollment_is_active as courserunenrollment_is_active
        , enrollment_mode as courserunenrollment_mode
        , certificate_id as courseruncertificate_id
        , certificate_user_id as courseruncertificate_user_id
        , certificate_key as courseruncertificate_key
        , certificate_mode as courseruncertificate_mode
        , certificate_distinction as courseruncertificate_distinction
        , certificate_grade as courseruncertificate_grade
        , certificate_download_url as courseruncertificate_download_url
        , certificate_download_uuid as courseruncertificate_download_uuid
        , certificate_verify_uuid as courseruncertificate_verify_uuid
        , certificate_name as courseruncertificate_name
        , certificate_status as courseruncertificate_status
        --- trino doesn't have function to convert first letter to upper case
        , regexp_replace(
            {{ transform_gender_value('profile_gender') }}, '(^[a-z])(.)', x -> upper(x[1]) || x[2] -- noqa
        ) as user_gender
        ,{{ transform_education_value('profile_level_of_education') }} as user_highest_education
        ,{{ cast_timestamp_to_iso8601('date_joined') }} as user_joined_on
        ,{{ cast_timestamp_to_iso8601('last_login') }} as user_last_login
        ,{{ cast_timestamp_to_iso8601('enrollment_created') }} as courserunenrollment_created_on
        ,{{ cast_timestamp_to_iso8601('certificate_created_date') }} as courseruncertificate_created_on
        ,{{ cast_timestamp_to_iso8601('certificate_modified_date') }} as courseruncertificate_updated_on
        , enrollment_course_id as courserunenrollment_courserun_readable_id
        , certificate_course_id as courseruncertificate_courserun_readable_id
    from source
    --- user_id could be blank due to parsing error on edx data so filter out these
    where user_id is not null
)

select * from cleaned
