with platforms as (select * from {{ ref("platforms") }})

select
    {{ dbt_utils.generate_surrogate_key(["id"]) }} as platform_pk,
    id as platform_readable_id,
    platform_name,
    platform_description,
    platform_domain
from platforms
