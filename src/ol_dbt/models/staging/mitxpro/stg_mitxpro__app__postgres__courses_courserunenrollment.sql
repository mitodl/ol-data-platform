select
    id
    , active
    , run_id
    , user_id
    , cast(created_on[1] as timestamp(6)) as created_on
    , cast(updated_on[1] as timestamp(6)) as updated_on
from
    {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__courses_courserunenrollment') }}
