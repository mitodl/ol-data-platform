-- MicroMasters User Employment Information

with source as (
    {{ live_raw_rows('raw__micromasters__app__postgres__profiles_employment') }}
)

, cleaned as (
    select
        id as user_employment_id
        , profile_id as user_profile_id
        , position as user_job_position
        , industry as user_company_industry
        , company_name as user_company_name
        , city as user_company_city
        , state_or_territory as user_company_state
        , country as user_company_country
        , {{ cast_date_to_iso8601('start_date') }} as user_start_date
        , {{ cast_date_to_iso8601('end_date') }} as user_end_date
    from source
)

select * from cleaned
