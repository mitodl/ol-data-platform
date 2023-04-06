select
    mitxonline_user_id as unique_field
    , count(*) as n_records

from dev.main_intermediate.int__openedx_mitxonline__users
where mitxonline_user_id is not null
group by mitxonline_user_id
having count(*) > 1
