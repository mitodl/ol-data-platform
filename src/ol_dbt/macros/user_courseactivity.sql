{#
  Bodies of the per-platform int__<platform>__user_courseactivity_<kind> and
  int__<platform>__user_courseactivities_daily models. Each platform reads the same
  Open edX tracking-log shape, so a model passes its own relation and the arguments
  below for the places the platforms differ.

  Args:
    activity_relation: ref() of the platform's user activity model
    user_id_column: the Open edX user id column, which is openedx_user_id on
      MITx Online and xPro and user_id on Residential and edX.org
    filter_null_courserun: false when activity_relation already drops the events
      that have no course run (int__edxorg__mitx_user_activity)
#}

{% macro user_courseactivity_source(activity_relation, filter_null_courserun=true) %}
with course_activities as (
    select * from {{ activity_relation }}
    {% if filter_null_courserun -%}
    where courserun_readable_id is not null
    {%- endif %}
)
{% endmacro %}

{% macro user_courseactivity_discussion(
    activity_relation, user_id_column='openedx_user_id', filter_null_courserun=true
) %}
{{ user_courseactivity_source(activity_relation, filter_null_courserun) }}
select
    user_username
    , courserun_readable_id
    , {{ user_id_column }}
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_path
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.id'") }} as useractivity_discussion_post_id
    , {{ json_query_string('useractivity_event_object', "'$.title'") }} as useractivity_discussion_post_title
    , {{ json_query_string('useractivity_event_object', "'$.category_id'") }} as useractivity_discussion_block_id
    , {{ json_query_string('useractivity_event_object', "'$.category_name'") }} as useractivity_discussion_block_name
    , {{ json_query_string('useractivity_event_object', "'$.url'") }} as useractivity_discussion_page_url
    , {{ json_query_string('useractivity_event_object', "'$.query'") }} as useractivity_discussion_search_query
    , {{ json_query_string('useractivity_event_object', "'$.user_forums_roles'") }} as useractivity_discussion_roles
from course_activities
where useractivity_event_type like 'edx.forum.%'
{% endmacro %}

{#
  distinct: true for Residential, the one platform whose model selects distinct rows.
#}
{% macro user_courseactivity_problemcheck(
    activity_relation, user_id_column='openedx_user_id', filter_null_courserun=true, distinct=false
) %}
{{ user_courseactivity_source(activity_relation, filter_null_courserun) }}
select{{ ' distinct' if distinct }}
    user_username
    , courserun_readable_id
    , {{ user_id_column }}
    , useractivity_event_type
    , useractivity_timestamp
    , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as useractivity_problem_name
    , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as useractivity_problem_id
    , {{ json_query_string('useractivity_event_object', "'$.answers'") }} as useractivity_problem_student_answers
    , {{ json_query_string('useractivity_event_object', "'$.attempts'") }} as useractivity_problem_attempts
    , {{ json_query_string('useractivity_event_object', "'$.success'") }} as useractivity_problem_success
    , {{ json_query_string('useractivity_event_object', "'$.grade'") }} as useractivity_problem_current_grade
    , {{ json_query_string('useractivity_event_object', "'$.max_grade'") }} as useractivity_problem_max_grade
from course_activities
where useractivity_event_type = 'problem_check'
--- This event emitted by the browser contain all of the GET parameters,
--  only events emitted by the server are useful
and useractivity_event_source = 'server'
{% endmacro %}

{% macro user_courseactivity_problemsubmitted(
    activity_relation, user_id_column='openedx_user_id', filter_null_courserun=true
) %}
{{ user_courseactivity_source(activity_relation, filter_null_courserun) }}
select
    user_username
    , courserun_readable_id
    , {{ user_id_column }}
    , useractivity_event_source
    , useractivity_event_type
    , useractivity_path
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.event_transaction_id'") }} as useractivity_event_id
    , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as useractivity_problem_name
    , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as useractivity_problem_id
    , {{ json_query_string('useractivity_event_object', "'$.weight'") }} as useractivity_problem_weight
    , {{ json_query_string('useractivity_event_object', "'$.weighted_earned'") }} as useractivity_problem_earned_score
    , {{ json_query_string('useractivity_event_object', "'$.weighted_possible'") }} as useractivity_problem_max_score
from course_activities
where useractivity_event_type = 'edx.grades.problem.submitted'
{% endmacro %}

{% macro user_courseactivity_showanswer(
    activity_relation, user_id_column='openedx_user_id', filter_null_courserun=true
) %}
{{ user_courseactivity_source(activity_relation, filter_null_courserun) }}
select
    user_username
    , courserun_readable_id
    , {{ user_id_column }}
    , useractivity_path
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as useractivity_problem_id
from course_activities
where useractivity_event_type = 'showanswer'
{% endmacro %}

{#
  include_event_object: Residential's model also exposes the raw event object.
#}
{% macro user_courseactivity_video(
    activity_relation, user_id_column='openedx_user_id', filter_null_courserun=true, include_event_object=false
) %}
{{ user_courseactivity_source(activity_relation, filter_null_courserun) }}
select
    user_username
    , courserun_readable_id
    , {{ user_id_column }}
    , useractivity_event_source
    , useractivity_event_type
    {% if include_event_object -%}
    , useractivity_event_object
    {% endif -%}
    , useractivity_page_url
    , useractivity_timestamp
    , {{ json_query_string('useractivity_event_object', "'$.id'") }} as useractivity_video_id
    , case
        when lower({{ json_query_string('useractivity_event_object', "'$.duration'") }}) = 'null' then null
        else cast({{ json_query_string('useractivity_event_object', "'$.duration'") }} as decimal(38, 4))
    end as useractivity_video_duration
    , {{ json_query_string('useractivity_event_object', "'$.currentTime'") }} as useractivity_video_currenttime
    , {{ json_query_string('useractivity_event_object', "'$.old_time'") }} as useractivity_video_old_time
    , {{ json_query_string('useractivity_event_object', "'$.new_time'") }} as useractivity_video_new_time
    , {{ json_query_string('useractivity_event_object', "'$.new_speed'") }} as useractivity_video_new_speed
    , {{ json_query_string('useractivity_event_object', "'$.old_speed'") }} as useractivity_video_old_speed
from course_activities
--- Some events have a url as useractivity_event_type. Keep only the video events listed in
--- https://edx.readthedocs.io/projects/devdata/en/latest/internal_data_formats/tracking_logs/student_event_types.html
--- #video-interaction-events
where
    {{ regexp_like('useractivity_event_type', "'(^[\\w]+)_video'") }} = true
    or {{ regexp_like('useractivity_event_type', "'(^[\\w]+)_transcript'") }} = true
    or useractivity_event_type like 'edx.video.%'
{% endmacro %}

{% macro user_courseactivities_daily(activity_relation, filter_null_courserun=true) %}
{{ user_courseactivity_source(activity_relation, filter_null_courserun) }}
, daily_activities_stats as (
    select
        user_username
        , courserun_readable_id
        , date({{ from_iso8601_timestamp('useractivity_timestamp') }}) as courseactivity_date
        , count(*) as courseactivity_num_events
    from course_activities
    group by
        user_username
        , courserun_readable_id
        , date({{ from_iso8601_timestamp('useractivity_timestamp') }})
)

select * from daily_activities_stats
{% endmacro %}
