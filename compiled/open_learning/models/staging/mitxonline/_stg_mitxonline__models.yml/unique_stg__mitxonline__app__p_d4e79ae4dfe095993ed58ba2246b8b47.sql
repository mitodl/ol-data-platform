select
    flexiblepricetier_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__flexiblepricing_flexiblepricetier
where flexiblepricetier_id is not null
group by flexiblepricetier_id
having count(*) > 1
