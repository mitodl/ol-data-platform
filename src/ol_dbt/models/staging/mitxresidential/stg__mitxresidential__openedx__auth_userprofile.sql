with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitx__openedx__mysql__auth_userprofile') }}
)

, cleaned as (
    select
        id as user_profile_id
        , user_id
        , replace(replace(replace(name, ' ', '<>'), '><', ''), '<>', ' ') as user_full_name
        , year_of_birth as user_birth_year
        , nullif(city, '') as user_address_city
        , nullif(country, '') as user_address_country
        , {{ transform_gender_value('gender') }} as user_gender
        , {{ transform_education_value('level_of_education') }} as user_highest_education
    from source
)

select * from cleaned
