with source as (

    select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__openedx__mysql__auth_userprofile') }}

)

, renamed as (

    select
        phone_number
        , mailing_address
        , location
        , bio
        , user_id
        , name
        , language
        , profile_image_uploaded_at
        , gender
        , level_of_education
        , meta
        , id
        , courseware
        , goals
        , country
        , year_of_birth
        , city
        , state

    from source

)

select * from renamed
