select
    currencyexchangerate_currency_code as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__flexiblepricing_currencyexchangerate
where currencyexchangerate_currency_code is not null
group by currencyexchangerate_currency_code
having count(*) > 1
