{{ config(materialized='view') }}

{{ user_courseactivity_discussion(ref('stg__mitxonline__openedx__tracking_logs__user_activity')) }}
