select
    flexiblepricetier_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__flexiblepricing_flexiblepricetier
where flexiblepricetier_id is not null
group by flexiblepricetier_id
having count(*) > 1
