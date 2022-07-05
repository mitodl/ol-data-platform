select
    id
    , active
    , run_id
    , user_id
    , created_on
    , updated_on
from
    {{ source('ol_warehouse_raw_data','mitxpro__app__postgres__courses_courserunenrollment') }}
