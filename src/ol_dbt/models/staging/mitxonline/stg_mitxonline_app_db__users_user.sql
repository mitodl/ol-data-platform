select
    id
    , created_on
    , is_active
from
    {{ source('ol_warehouse_raw_data','mitxonline__app__postgres__users_user') }}
