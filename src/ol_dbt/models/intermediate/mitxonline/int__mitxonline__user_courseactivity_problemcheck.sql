{{ config(materialized='view') }}

{{ user_courseactivity_problemcheck(ref('stg__mitxonline__openedx__tracking_logs__user_activity')) }}
