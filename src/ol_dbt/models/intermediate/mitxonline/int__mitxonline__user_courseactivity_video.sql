{{ config(materialized='view') }}

{{ user_courseactivity_video(ref('stg__mitxonline__openedx__tracking_logs__user_activity')) }}
