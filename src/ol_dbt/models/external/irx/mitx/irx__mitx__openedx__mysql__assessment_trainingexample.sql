with
    te as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_trainingexample") }}
    ),
    ate as (
        select *
        from
            {{
                source(
                    "ol_warehouse_raw_data",
                    "raw__mitx__openedx__mysql__assessment_aitrainingworkflow_training_examples",
                )
            }}
    ),
    tw as (
        select * from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_aitrainingworkflow") }}
    ),
    stwi as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_studenttrainingworkflowitem") }}
    ),
    stw as (
        select *
        from {{ source("ol_warehouse_raw_data", "raw__mitx__openedx__mysql__assessment_studenttrainingworkflow") }}
    )

select distinct te.*
from te
inner join ate on te.id = ate.trainingexample_id
inner join tw on ate.aitrainingworkflow_id = tw.id
union distinct
select distinct te.*
from te
inner join stwi on te.id = stwi.training_example_id
inner join stw on stwi.workflow_id = stw.id
