-- Platform information for MITxPro
with platforms as (select * from {{ ref("stg__mitxpro__app__postgres__courses_platform") }})

select platform_id, platform_name
from platforms
