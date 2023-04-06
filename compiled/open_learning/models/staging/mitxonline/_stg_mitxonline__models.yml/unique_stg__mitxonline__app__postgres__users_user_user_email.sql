select
    user_email as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxonline__app__postgres__users_user
where user_email is not null
group by user_email
having count(*) > 1
