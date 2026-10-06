{{ config(materialized='view') }}

{{ user_courseactivity_video(
    ref('int__edxorg__mitx_user_activity'),
    user_id_column='user_id',
    filter_null_courserun=false
) }}
