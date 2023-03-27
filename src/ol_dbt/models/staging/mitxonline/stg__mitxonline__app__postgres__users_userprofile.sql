with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__app__postgres__users_userprofile') }}
)

, cleaned as (
    select
        id as user_profile_id
        , user_id
        , year_of_birth as user_birth_year
        , company as user_company
        , industry as user_industry
        , job_title as user_job_title
        , job_function as user_job_function
        , leadership_level as user_leadership_level
        , highest_education as user_highest_education
        , type_is_student as user_type_is_student
        , type_is_professional as user_type_is_professional
        , type_is_educator as user_type_is_educator
        , type_is_other as user_type_is_other
        , {{ transform_gender_value('gender') }} as user_gender
        , {{ transform_company_size_value('company_size') }} as user_company_size
        , {{ transform_years_experience_value('years_experience') }} as user_years_experience
        , {{ cast_timestamp_to_iso8601('created_on') }} as user_profile_created_on
        , {{ cast_timestamp_to_iso8601('updated_on') }} as user_profile_updated_on
    from source
)

select * from cleaned
