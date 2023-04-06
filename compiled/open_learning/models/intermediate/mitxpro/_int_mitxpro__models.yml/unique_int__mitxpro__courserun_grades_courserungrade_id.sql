select
    courserungrade_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__courserun_grades
where courserungrade_id is not null
group by courserungrade_id
having count(*) > 1
