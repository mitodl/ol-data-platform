select
    user_mitxonline_username as user_username
    , full_name as user_full_name
    , email as user_email
    , address_country as user_address_country
    , address_state as user_address_state
    , birth_year as user_birth_year
    , gender as user_gender
    , highest_education as user_highest_education
    , latest_income_usd
    , latest_original_income
    , latest_original_currency
from {{ ref('dim_user') }}
where user_mitxonline_username is not null
    -- exclude MicroMasters-only users: they may have user_mitxonline_username populated
    -- via a MicroMasters linkage but have never created a MITxOnline application account
    and mitxonline_application_user_id is not null
