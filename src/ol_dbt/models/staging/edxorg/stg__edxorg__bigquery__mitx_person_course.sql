-- Contains user profile, enrollments, grades and certificates Information for edx.org


with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__irx__edxorg__bigquery__mitx_person_course') }}

)

, renamed as (

    select
        user_id
        , course_id as courserun_readable_id

        --- users
        , username as user_username
        , yob as user_birth_year
        , profile_country as user_profile_country

        --- enrollments, grades and certificates
        , mode as courserunenrollment_enrollment_mode
        , passing_grade as courserungrade_passing_grade
        , grade as courserungrade_user_grade
        , completed as courserungrade_is_passing
        , certified as courseruncertificate_is_earned
        , cert_status as courseruncertificate_status
        , coalesce(is_active = 1, false) as courserunenrollment_is_active
        , {{ transform_gender_value('gender') }} as user_gender
        , {{ transform_education_value('loe') }} as user_highest_education

        , to_iso8601(from_iso8601_timestamp(start_time)) as courserunenrollment_created_on
        , to_iso8601(from_iso8601_timestamp(verified_enroll_time)) as courserunenrollment_enrolled_on

        , to_iso8601(from_iso8601_timestamp(verified_unenroll_time)) as courserunenrollment_unenrolled_on
        , to_iso8601(from_iso8601_timestamp(cert_created_date)) as courseruncertificate_created_on
        , to_iso8601(from_iso8601_timestamp(cert_modified_date)) as courseruncertificate_updated_on
    from source
    where user_id is not null --- temporary fix to filter the bad data

)

select * from renamed
