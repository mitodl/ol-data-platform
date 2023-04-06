select
    line_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_line
where line_id is not null
group by line_id
having count(*) > 1
