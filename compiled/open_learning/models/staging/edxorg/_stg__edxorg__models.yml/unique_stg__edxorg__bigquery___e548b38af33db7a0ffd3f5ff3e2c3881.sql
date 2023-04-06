select
    courserun_readable_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__edxorg__bigquery__mitx_courserun
where courserun_readable_id is not null
group by courserun_readable_id
having count(*) > 1
