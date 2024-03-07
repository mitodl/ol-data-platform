with users as (
    select * from {{ ref('int__mitxonline__users') }}
)

, user_income as (
    select
        user_id
        , flexiblepriceapplication_income_usd
        , flexiblepriceapplication_original_income
        , flexiblepriceapplication_original_currency
        , rank() over (partition by user_id order by flexiblepriceapplication_updated_on desc) as rnk
    from int__mitxonline__flexiblepricing_flexiblepriceapplication
)


select
    users.user_username
    , users.user_full_name
    , users.user_email
    , users.user_address_country
    , users.user_address_state
    , users.user_birth_year
    , users.user_gender
    , users.user_highest_education
    , user_income.flexiblepriceapplication_income_usd as latest_income_usd
    , user_income.flexiblepriceapplication_original_income as latest_original_income
    , user_income.flexiblepriceapplication_original_currency as latest_original_currency
from users
left join user_income
    on
        users.user_id = users.user_id
        and user_income.rnk = 1
