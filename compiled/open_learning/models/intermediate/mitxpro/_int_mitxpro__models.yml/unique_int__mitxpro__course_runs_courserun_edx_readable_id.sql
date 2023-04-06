select
    courserun_edx_readable_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__course_runs
where courserun_edx_readable_id is not null
group by courserun_edx_readable_id
having count(*) > 1
