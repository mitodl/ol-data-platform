with source as (
    select * from dev.main_staging.stg__mitxonline__app__postgres__flexiblepricing_currencyexchangerate
)

select
    currencyexchangerate_id
    , currencyexchangerate_created_on
    , currencyexchangerate_updated_on
    , currencyexchangerate_description
    , currencyexchangerate_currency_code
    , currencyexchangerate_exchange_rate
from source
