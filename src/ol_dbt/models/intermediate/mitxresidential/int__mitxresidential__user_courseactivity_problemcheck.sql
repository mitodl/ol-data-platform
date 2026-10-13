{{ config(materialized='view') }}

{{ user_courseactivity_problemcheck(
    ref('stg__mitxresidential__openedx__tracking_logs__user_activity'),
    user_id_column='user_id',
    distinct=true
) }}
