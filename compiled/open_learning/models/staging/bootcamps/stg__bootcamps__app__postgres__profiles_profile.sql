with source as (
    select * from dev.main_raw.raw__bootcamps__app__postgres__profiles_profile
)

, cleaned as (
    select
        id as user_profile_id
        , name as user_full_name
        , birth_year as user_birth_year
        , company as user_company
        , job_title as user_job_title
        , industry as user_industry
        , job_function as user_job_function
        , user_id
        , highest_education as user_highest_education
        ,
        case
            when gender = 'm' then 'Male'
            when gender = 'f' then 'Female'
            when gender = 't' then 'Transgender'
            when gender = 'nb' then 'Non-binary/non-conforming'
            when gender = 'o' then 'Other/Prefer Not to Say'
            else gender
        end
        as user_gender
        , case
            when company_size = 1 then 'Small/Start-up (1+ employees)'
            when company_size = 9 then 'Small/Home office (1-9 employees)'
            when company_size = 99 then 'Small (10-99 employees)'
            when company_size = 999 then 'Small to medium-sized (100-999 employees)'
            when company_size = 9999 then 'Medium-sized (1000-9999 employees)'
            when company_size = 10000 then 'Large Enterprise (10,000+ employees)'
            when company_size = 0 then 'Other (N/A or Don''t know)'
            else cast(company_size as varchar)
        end as user_company_size
        , case
            when years_experience = 2 then 'Less than 2 years'
            when years_experience = 5 then '2-5 years'
            when years_experience = 10 then '6 - 10 years'
            when years_experience = 15 then '11 - 15 years'
            when years_experience = 20 then '16 - 20 years'
            when years_experience = 21 then 'More than 20 years'
            when years_experience = 0 then 'Prefer not to say'
            else cast(years_experience as varchar)
        end as user_years_experience
    from source
)

select * from cleaned
