select
    id
    , cast(created_on[1] as timestamp(6)) as created_on
    , cast(updated_on[1] as timestamp(6)) as updated_on
    , program_id
    , user_id
    , company_id
    , active
    , change_status
    , order_id
from
    {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__courses_programenrollment') }}
