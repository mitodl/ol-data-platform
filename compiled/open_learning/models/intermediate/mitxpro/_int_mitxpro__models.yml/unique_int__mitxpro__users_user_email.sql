select
    user_email as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__mitxpro__users
where user_email is not null
group by user_email
having count(*) > 1
