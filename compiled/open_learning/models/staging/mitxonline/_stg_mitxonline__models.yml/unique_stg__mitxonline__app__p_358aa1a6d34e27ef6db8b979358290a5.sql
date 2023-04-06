select
    programrequirement_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_programrequirement
where programrequirement_id is not null
group by programrequirement_id
having count(*) > 1
