select
    id
    , live
    , title
    , course_id
    , courseware_url_path
    , created_on
    , is_active
from
    {{ source('ol_warehouse_raw_data','mitxonline__app__postgres__courses_courserun') }}
