{{ config(materialized='view') }}

{{ user_courseactivity_problemcheck(ref('stg__mitxpro__openedx__tracking_logs__user_activity')) }}
