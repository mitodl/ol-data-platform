select
    id
    , classifier_set_id
    , classifier_data
from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_aiclassifier') }}
