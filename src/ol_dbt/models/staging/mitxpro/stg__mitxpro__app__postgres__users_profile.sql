-- xPro User Information
with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__xpro__app__postgres__users_profile") }}),
    cleaned as (
        select
            id as user_profile_id,
            birth_year as user_birth_year,
            company as user_company,
            job_title as user_job_title,
            industry as user_industry,
            job_function as user_job_function,
            leadership_level as user_leadership_level,
            user_id,
            highest_education as user_highest_education,
            {{ transform_gender_value("gender") }} as user_gender,
            {{ transform_company_size_value("company_size") }} as user_company_size,
            {{ transform_years_experience_value("years_experience") }} as user_years_experience
        from source
    )

select *
from cleaned
