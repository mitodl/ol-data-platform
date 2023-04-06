select
    program_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__programs
where program_id is not null
group by program_id
having count(*) > 1
