select
    user_username as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__edxorg__mitx_users
where user_username is not null
group by user_username
having count(*) > 1
