select
    courserun_readable_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__course_runs
where courserun_readable_id is not null
group by courserun_readable_id
having count(*) > 1
