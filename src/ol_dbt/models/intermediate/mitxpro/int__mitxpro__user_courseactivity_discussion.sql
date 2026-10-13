{{ config(materialized='view') }}

{{ user_courseactivity_discussion(ref('stg__mitxpro__openedx__tracking_logs__user_activity')) }}
