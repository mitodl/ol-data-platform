select
    user_id as unique_field
    , count(*) as n_records

from dev.main_staging.stg__mitxpro__app__postgres__users_legaladdress
where user_id is not null
group by user_id
having count(*) > 1
