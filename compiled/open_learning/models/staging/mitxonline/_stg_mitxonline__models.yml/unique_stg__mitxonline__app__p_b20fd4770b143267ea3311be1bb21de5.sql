select
    program_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__courses_program
where program_id is not null
group by program_id
having count(*) > 1
