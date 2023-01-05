-- Contains user profile, enrollments, grades and certificates Information for edx.org


with source as (

    select *
    from
        {{ source('ol_warehouse_raw_data', 'raw__irx__edxorg__bigquery__mitx_person_course') }}

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
        , case
            when gender = 'm' then 'Male'
            when gender = 'f' then 'Female'
            when gender = 'o' then 'Other/Prefer Not to Say'
            else gender
        end as user_gender
        , case
            when loe = 'p' then 'Doctorate'
            when loe = 'm' then 'Master''s or professional degree'
            when loe = 'b' then 'Bachelor''s degree'
            when loe = 'a' then 'Associate degree'
            when loe = 'hs' then 'Secondary/high school'
            when loe = 'jhs' then 'Junior secondary/junior high/middle school'
            when loe = 'el' then 'Elementary/primary school'
            when loe = 'none' then 'No formal education'
            when loe = 'other' or loe = 'o' then 'Other education'
            --- the following two are no longer used, but there are still user's profiles have these values
            when loe = 'p_se' then 'Doctorate in science or engineering'
            when loe = 'p_oth' then 'Doctorate in another field'
            else loe
        end as user_highest_education

        , to_iso8601(
            from_iso8601_timestamp(start_time)
        ) as courserunenrollment_created_on
        , to_iso8601(
            from_iso8601_timestamp(verified_enroll_time)
        ) as courserunenrollment_enrolled_on

        , to_iso8601(
            from_iso8601_timestamp(verified_unenroll_time)
        ) as courserunenrollment_unenrolled_on
        , to_iso8601(
            from_iso8601_timestamp(cert_created_date)
        ) as courseruncertificate_created_on
        , to_iso8601(
            from_iso8601_timestamp(cert_modified_date)
        ) as courseruncertificate_updated_on
    from source
    where user_id is not null --- temporary fix to filter the bad data

)

select * from renamed
