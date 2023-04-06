select
    flexiblepriceapplication_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__flexiblepricing_flexiblepriceapplication
where flexiblepriceapplication_id is not null
group by flexiblepriceapplication_id
having count(*) > 1
