-- MicroMasters User Educations Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__micromasters__app__postgres__profiles_education') }}
)

, cleaned as (
    select
        id as user_education_id
        , profile_id as user_profile_id
        , field_of_study as user_field_of_study
        , online_degree as user_education_is_online_degree
        , school_city as user_school_city
        , school_country as user_school_country
        , school_name as user_school_name
        , school_state_or_territory as user_school_state
        , {{ transform_education_value('degree_name') }} as user_education_degree
        , {{ cast_date_to_iso8601('graduation_date') }} as user_graduation_date
    from source
)

select * from cleaned
