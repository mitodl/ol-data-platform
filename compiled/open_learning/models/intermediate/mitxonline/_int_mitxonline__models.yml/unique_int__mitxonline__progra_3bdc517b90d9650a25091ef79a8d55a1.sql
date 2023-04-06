select
    programrequirement_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxonline__program_to_courses
where programrequirement_id is not null
group by programrequirement_id
having count(*) > 1
