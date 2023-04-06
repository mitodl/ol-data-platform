select
    countryincomethreshold_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__flexiblepricing_countryincomethreshold
where countryincomethreshold_id is not null
group by countryincomethreshold_id
having count(*) > 1
