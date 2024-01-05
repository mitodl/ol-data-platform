with tos as (
    select *
    from
        {{ source (
            'ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_trainingexample_options_selected'
        ) }}
)

, te as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_trainingexample') }}
)

, ate as (
    select *
    from
        {{ source(
            'ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_aitrainingworkflow_training_examples'
        ) }}
)

, tw as (
    select * from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_aitrainingworkflow') }}
)

, stwi as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_studenttrainingworkflowitem') }}
)

, stw as (
    select *
    from {{ source('ol_warehouse_raw_data','raw__mitxonline__openedx__mysql__assessment_studenttrainingworkflow') }}
)

select
    tos.trainingexample_id
    , tos.id
    , tos.criterionoption_id
from tos
where tos.trainingexample_id in (
    select distinct te.id from te
    left join ate on te.id = ate.trainingexample_id
    left join tw on ate.aitrainingworkflow_id = tw.id
    union distinct
    select distinct te.id from te
    left join stwi on te.id = stwi.training_example_id
    left join stw on stwi.workflow_id = stw.id
)
