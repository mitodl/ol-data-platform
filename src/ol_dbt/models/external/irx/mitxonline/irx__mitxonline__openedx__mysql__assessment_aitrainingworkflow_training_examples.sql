select
    trainingexample_id
    , id
    , aitrainingworkflow_id
from
    {{ source(
        'ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_aitrainingworkflow_training_examples'
    ) }}
