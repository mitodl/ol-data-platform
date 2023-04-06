select
    currencyexchangerate_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__flexiblepricing_currencyexchangerate
where currencyexchangerate_id is not null
group by currencyexchangerate_id
having count(*) > 1
