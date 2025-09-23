-- MicroMasters User Profile Information
with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__micromasters__app__postgres__profiles_profile") }}
    ),
    cleaned as (
        select
            id as user_profile_id,
            user_id,
            country as user_address_country,
            birth_country as user_birth_country,
            city as user_address_city,
            state_or_territory as user_address_state_or_territory,
            postal_code as user_address_postal_code,
            address as user_street_address,
            preferred_name as user_preferred_name,
            mail_id as user_mail_id,
            student_id as user_student_id,
            email_optin as user_email_is_optin,
            filled_out as user_profile_is_filled_out,
            image as user_profile_image,
            image_small as user_profile_image_small,
            image_medium as user_profile_image_medium,
            nationality as user_nationality,
            phone_number as user_phone_number,
            edx_employer as user_employer,
            edx_job_title as user_job_title,
            preferred_language as user_preferred_language,
            edx_mailing_address as user_mailing_address,
            verified_micromaster_user as user_is_verified,
            agreed_to_terms_of_service as user_has_agreed_to_terms_of_service,
            edx_language_proficiencies as user_language_proficiencies,
            edx_requires_parental_consent as user_profile_parental_consent_is_required,
            edx_bio as user_bio,
            about_me as user_about_me,
            edx_name as user_edx_name,
            edx_goals as user_edx_goals,
            fake_user as user_profile_is_fake,
            {{ cast_date_to_iso8601("date_of_birth") }} as user_birth_date,
            coalesce(romanized_first_name, first_name) as user_first_name,
            coalesce(romanized_last_name, last_name) as user_last_name,
            replace(
                replace(
                    replace((concat_ws(chr(32), nullif(first_name, ''), nullif(last_name, ''))), ' ', '<>'), '><', ''
                ),
                '<>',
                ' '
            ) as user_full_name,
            concat_ws(
                chr(32), nullif(romanized_first_name, ''), nullif(romanized_last_name, '')
            ) as user_romanized_full_name,
            {{ transform_education_value("edx_level_of_education") }} as user_highest_education,
            {{ transform_gender_value("gender") }} as user_gender,
            case
                when account_privacy = 'public'
                then 'Public to everyone'
                when account_privacy = 'public_to_mm'
                then 'Public to logged in users'
                when account_privacy = 'private'
                then 'Private'
                else account_privacy
            end as user_account_privacy,
            {{ cast_timestamp_to_iso8601("date_joined_micromasters") }} as user_joined_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as user_profile_updated_on
        from source
    )

select *
from cleaned
