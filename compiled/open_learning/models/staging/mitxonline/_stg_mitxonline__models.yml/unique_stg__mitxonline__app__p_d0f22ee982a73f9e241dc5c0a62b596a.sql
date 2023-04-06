select
    courserun_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_courserun
where courserun_id is not null
group by courserun_id
having count(*) > 1
