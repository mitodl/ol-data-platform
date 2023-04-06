select
    courserungrade_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxresidential__openedx__courserun_grade
where courserungrade_id is not null
group by courserungrade_id
having count(*) > 1
