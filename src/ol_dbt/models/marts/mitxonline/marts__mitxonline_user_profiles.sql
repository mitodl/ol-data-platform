{{ config(tags=['IRx']) }}

with users as (
    select * from {{ ref('int__mitxonline__users') }}
)

select
    user_username
    , user_full_name
    , user_email
    , user_address_country
    , user_address_state
    , user_birth_year
    , user_gender
    , user_highest_education
from users
