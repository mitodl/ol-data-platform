-- MicroMasters User Social Auth Information
-- Authentication backend can either be MITxOnline or edx.org
-- Can be used to match MITxOnline users. However,
-- not every user from MITxOnline DB has a matching record in this source table
with
    source as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__micromasters__app__postgres__social_auth_usersocialauth") }}
    ),
    cleaned as (select id as user_auth_id, user_id, uid as user_username, provider as user_auth_provider from source)

select *
from cleaned
