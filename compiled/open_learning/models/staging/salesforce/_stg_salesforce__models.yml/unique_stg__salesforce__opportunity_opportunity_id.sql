select
    opportunity_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__salesforce__opportunity
where opportunity_id is not null
group by opportunity_id
having count(*) > 1
