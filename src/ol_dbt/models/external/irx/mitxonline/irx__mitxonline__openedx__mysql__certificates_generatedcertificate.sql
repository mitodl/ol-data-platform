select
    modified_date
    , mode
    , status
    , error_reason
from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__certificates_generatedcertificate') }}