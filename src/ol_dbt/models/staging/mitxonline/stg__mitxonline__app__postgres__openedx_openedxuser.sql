with
    source as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__app__postgres__openedx_openedxuser") }}
    ),
    cleaned as (

        select
            id as openedxuser_id,
            user_id,
            platform as openedxuser_platform,
            edx_username as openedxuser_username,
            desired_edx_username as openedxuser_desired_username,
            has_been_synced as openedxuser_has_been_synced,
            {{ cast_timestamp_to_iso8601("created_on") }} as openedxuser_created_on,
            {{ cast_timestamp_to_iso8601("updated_on") }} as openedxuser_updated_on
        from source
    )

select *
from cleaned
