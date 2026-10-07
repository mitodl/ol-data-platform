{{ config(materialized='view') }}

{{ user_courseactivity_problemsubmitted(
    ref('stg__mitxresidential__openedx__tracking_logs__user_activity'),
    user_id_column='user_id'
) }}
