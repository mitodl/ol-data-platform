select
    countryincomethreshold_country_code as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__flexiblepricing_countryincomethreshold
where countryincomethreshold_country_code is not null
group by countryincomethreshold_country_code
having count(*) > 1
