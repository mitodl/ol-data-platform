select
    id
    , created_on
    , is_active
from {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__users_user') }}
