select
    programenrollment_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__programenrollments
where programenrollment_id is not null
group by programenrollment_id
having count(*) > 1
