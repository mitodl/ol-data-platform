select
    user_username as unique_field
    , count(*) as n_records

from dev.main_staging.stg__bootcamps__app__postgres__auth_user
where user_username is not null
group by user_username
having count(*) > 1
