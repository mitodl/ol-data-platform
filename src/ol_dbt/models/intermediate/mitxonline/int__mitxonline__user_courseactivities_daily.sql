-- enrollment events sent through the edX API have an empty username, so take it from auth_user
{% set course_activities %}
(
    select
        coalesce(nullif(activity.user_username, ''), auth_user.user_username) as user_username
        , activity.courserun_readable_id
        , activity.useractivity_timestamp
    from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }} as activity
    left join {{ ref('stg__mitxonline__openedx__mysql__auth_user') }} as auth_user
        on activity.user_username = '' and activity.openedx_user_id = auth_user.openedx_user_id
    where activity.user_username != '' or auth_user.user_username is not null
) as activity
{% endset %}

{{ user_courseactivities_daily(course_activities) }}
