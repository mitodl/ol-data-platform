select
    programrun_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__program_runs
where programrun_id is not null
group by programrun_id
having count(*) > 1
