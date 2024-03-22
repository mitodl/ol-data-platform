with certificates_generatedcertificate as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__certificates_generatedcertificate') }}
)

{{ deduplicate_query('certificates_generatedcertificate', 'most_recent_source') }}

select
    id
    , user_id
    , course_id
    , key
    , name
    , distinction
    , verify_uuid
    , download_uuid
    , download_url
    , grade
    , created_date
    , modified_date
    , mode
    , status
    , error_reason
from most_recent_source
