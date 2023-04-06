select
    mitxpro_user_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__openedx_mitxpro__users
where mitxpro_user_id is not null
group by mitxpro_user_id
having count(*) > 1
