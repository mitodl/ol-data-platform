with platforms as (
    select * from {{ ref('platforms') }}
)

select
    {{ generate_hash_id('id') }} as platform_id
    , platform_name
    , platform_description
    , platform_domain
from platforms
