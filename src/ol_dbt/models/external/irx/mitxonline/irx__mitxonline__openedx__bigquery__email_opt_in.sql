with email_opt_in as (
    select * from {{ ref('int__mitxonline__bulk_email_optin') }}
)

select
    is_opted_in_for_email
    , course_id
    , full_name
    , preference_set_datetime
    , user_id
from email_opt_in
