-- MIT Learn User Profile Information

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitlearn__app__postgres__profiles_profile') }}
)

, cleaned as (
    select
        id as profile_id
        , user_id
        , email_optin as user_email_optin
        , toc_optin as user_toc_optin
        , certificate_desired as user_certificate_desired
        , completed_onboarding as user_completed_onboarding
        , nullif(name, '') as user_name
        , nullif(bio, '') as user_bio
        , nullif(goals, array[]) as user_goals
        , nullif(headline, '') as user_headline
        , nullif(location, '') as user_location
        , nullif(delivery, array[]) as user_delivery_preference
        , nullif(current_education, '') as user_current_education
        , nullif(time_commitment, '') as user_time_commitment
        , nullif(image, '') as user_image_url
        , nullif(image_small, '') as user_image_small_url
        , nullif(image_medium, '') as user_image_medium_url
        , nullif(image_file, '') as user_image_file
        , nullif(image_small_file, '') as user_image_small_file
        , nullif(image_medium_file, '') as user_image_medium_file
        , {{ cast_timestamp_to_iso8601('updated_at') }} as profile_updated_on
    from source
)

select * from cleaned
