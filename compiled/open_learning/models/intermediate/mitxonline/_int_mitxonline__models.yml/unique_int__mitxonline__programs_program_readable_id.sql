select
    program_readable_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__programs
where program_readable_id is not null
group by program_readable_id
having count(*) > 1
