select
    programrunline_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_programrunline
where programrunline_id is not null
group by programrunline_id
having count(*) > 1
