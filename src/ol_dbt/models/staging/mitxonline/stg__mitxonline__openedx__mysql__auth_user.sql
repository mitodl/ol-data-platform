-- MITx Online open edX User Information
with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__openedx__mysql__auth_user") }}),
    cleaned as (

        select
            id as openedx_user_id,
            username as user_username,
            email as user_email,
            is_active as user_is_active,
            is_staff as user_is_staff,
            is_superuser as user_is_superuser
        from source
    )

select *
from cleaned
