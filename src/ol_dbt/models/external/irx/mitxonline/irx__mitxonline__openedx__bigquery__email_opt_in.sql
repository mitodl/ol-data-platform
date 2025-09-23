with email_opt_in as (select * from {{ ref("int__mitxonline__bulk_email_optin") }})

select
    openedx_user_id, user_full_name, user_username, user_email, courserun_readable_id, courserun_title, email_opted_in
from email_opt_in
