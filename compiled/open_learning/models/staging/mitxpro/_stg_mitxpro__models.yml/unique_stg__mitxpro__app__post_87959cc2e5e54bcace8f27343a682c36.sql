select
    programrun_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__courses_programrun
where programrun_id is not null
group by programrun_id
having count(*) > 1
