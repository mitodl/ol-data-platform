-- It contains user profiles, enrollments and certificate detail of learners in a given course
-- There are some fields overlapping with person_course

with source as (
    select *
    from dev.main_raw.raw__irx__edxorg__bigquery__mitx_user_info_combo
)

, cleaned as (

    select
        user_id
        ,
        email as user_email
        , username as user_username
        , id_map_hash_id as user_map_hash_id
        , profile_name as user_full_name
        , profile_mailing_address as user_mailing_address
        , profile_goals as user_profile_goals
        , profile_city as user_city
        , profile_year_of_birth as user_birth_year
        -- filter outcountry values which are not valid country codes which consist of
        -- two capital letters
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
        , case
            when enrollment_course_id like 'MITxT%' then 'MITx Online'
            ---- only course_id starts with xPRO are from xPro open edx platform
            when enrollment_course_id like 'xPRO%' then 'xPro'
            --- Some runs from course - VJx Visualizing Japan (1850s-1930s) that run on edx don't start with 'MITx/`
            --- e.g. VJx/VJx_S/3T2015, VJx/VJx/3T2014, VJx/VJx_2/3T2016
            when enrollment_course_id like 'MITx/%' or enrollment_course_id like 'VJx%' then 'edX.org'
        end
        as courserun_platform
        , case when regexp_like(profile_country, '^[A-Z]{2}$') then profile_country end as user_country
        ,
        case
            when profile_gender = 'm' then 'Male'
            when profile_gender = 'f' then 'Female'
            when profile_gender = 't' then 'Transgender'
            when profile_gender = 'nb' then 'Non-binary/non-conforming'
            when profile_gender = 'o' then 'Other/Prefer Not to Say'
            else profile_gender
        end
        as user_gender
        ,
        case
            when profile_level_of_education = 'p' then 'Doctorate'
            when profile_level_of_education = 'm' then 'Master''s or professional degree'
            when profile_level_of_education = 'b' then 'Bachelor''s degree'
            when profile_level_of_education = 'a' then 'Associate degree'
            when profile_level_of_education = 'hs' then 'Secondary/high school'
            when profile_level_of_education = 'jhs' then 'Junior secondary/junior high/middle school'
            when profile_level_of_education = 'el' then 'Elementary/primary school'
            when profile_level_of_education = 'none' then 'No formal education'
            when profile_level_of_education = 'other' or profile_level_of_education = 'o' then 'Other education'
            --- the following two are no longer used, but there are still user's profiles have these values
            when profile_level_of_education = 'p_se' then 'Doctorate in science or engineering'
            when profile_level_of_education = 'p_oth' then 'Doctorate in another field'
            else profile_level_of_education
        end
        as user_highest_education
        ,
        to_iso8601(from_iso8601_timestamp(date_joined))
        as user_joined_on
        ,
        to_iso8601(from_iso8601_timestamp(last_login))
        as user_last_login
        ,
        to_iso8601(from_iso8601_timestamp(enrollment_created))
        as courserunenrollment_created_on
        ,
        to_iso8601(from_iso8601_timestamp(certificate_created_date))
        as courseruncertificate_created_on
        ,
        to_iso8601(from_iso8601_timestamp(certificate_modified_date))
        as courseruncertificate_updated_on
        , replace(enrollment_course_id, 'ESD.SCM1x', 'CTL.SC1x') as courserunenrollment_courserun_readable_id
        , replace(certificate_course_id, 'ESD.SCM1x', 'CTL.SC1x') as courseruncertificate_courserun_readable_id
    from source
    --- user_id could be blank due to parsing error on edx data so filter out these
    where user_id is not null
)

select * from cleaned
