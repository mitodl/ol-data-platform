select
    linerunselection_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__ecommerce_linerunselection
where linerunselection_id is not null
group by linerunselection_id
having count(*) > 1
