select
    blockedcountry_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_blockedcountry
where blockedcountry_id is not null
group by blockedcountry_id
having count(*) > 1
