select
    courseaccessrole_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxresidential__openedx__user_courseaccessrole
where courseaccessrole_id is not null
group by courseaccessrole_id
having count(*) > 1
