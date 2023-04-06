select
    programrun_readable_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__program_runs
where programrun_readable_id is not null
group by programrun_readable_id
having count(*) > 1
