with email_opt_in as (
    select * from {{ source('ol_warehouse_raw_data','raw__irx__edxorg__bigquery__email_opt_in') }}
)

select
    is_opted_in_for_email
    , course_id
    , full_name
    , preference_set_datetime
    , user_id
from email_opt_in
