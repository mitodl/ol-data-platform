{% macro generate_studentmodule_problem_events(studentmodule_table, studentmodulehistory_table, user_id_field) %}
(
  with studentmodule as (
    select
      studentmodule_id
      , courserun_readable_id
      , coursestructure_block_id
      , coursestructure_block_category
      , {{ user_id_field }} as user_id
      , studentmodule_state_data
      , studentmodule_problem_grade
      , studentmodule_problem_max_grade
      , from_iso8601_timestamp_nanos(studentmodule_created_on) as studentmodule_created_on
      , from_iso8601_timestamp_nanos(studentmodule_updated_on) as studentmodule_updated_on
    from {{ studentmodule_table }}
    where coursestructure_block_category = 'problem'
  )

  {% if studentmodulehistory_table != 'null' %}
  , studentmodulehistoryextended as (
    select
      studentmodulehistoryextended_id
      , studentmodule_id
      , studentmodule_state_data
      , studentmodule_problem_grade
      , studentmodule_problem_max_grade
      , from_iso8601_timestamp_nanos(cast(studentmodule_created_on as varchar)) as studentmodule_created_on
    from {{ studentmodulehistory_table }}
  )
  {% endif %}

  , base as (
    select
      sm.user_id
      , sm.courserun_readable_id
      , sm.studentmodule_id
      , sm.coursestructure_block_id
      , sm.studentmodule_updated_on
      {% if studentmodulehistory_table != 'null' %}
      , coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data) as studentmodule_state_data
      , coalesce(smhe.studentmodule_problem_grade, sm.studentmodule_problem_grade) as studentmodule_problem_grade
      , coalesce(smhe.studentmodule_problem_max_grade, sm.studentmodule_problem_max_grade) as studentmodule_problem_max_grade
      , cast(json_parse(json_query(coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data), 'lax $.correct_map_history')) as array(json)) as correct_history
      , cast(json_parse(json_query(coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data), 'lax $.student_answers_history')) as array(json)) as answer_history
      from studentmodule sm
      left join studentmodulehistoryextended smhe
        on sm.studentmodule_id = smhe.studentmodule_id
      {% else %}
      , sm.studentmodule_state_data
      , sm.studentmodule_problem_grade
      , sm.studentmodule_problem_max_grade
      , cast(json_parse(json_query(sm.studentmodule_state_data, 'lax $.correct_map_history')) as array(json)) as correct_history
      , cast(json_parse(json_query(sm.studentmodule_state_data, 'lax $.student_answers_history')) as array(json)) as answer_history
      from studentmodule sm
      {% endif %}
  )

  , indexed as (
    select
      user_id
      , courserun_readable_id
      , studentmodule_id
      , studentmodule_state_data
      , coursestructure_block_id
      , studentmodule_problem_grade
      , studentmodule_problem_max_grade
      , studentmodule_updated_on
      , correct_history[idx] as correct_item
      , answer_history[idx] as answer_item
      , idx as attempt
    from base
    cross join unnest(sequence(1, cardinality(correct_history))) as t(idx)
  )

  , exploded as (
    select
      user_id
      , courserun_readable_id
      , studentmodule_id
      , studentmodule_state_data
      , coursestructure_block_id
      , studentmodule_problem_grade
      , studentmodule_problem_max_grade
      , studentmodule_updated_on
      , attempt
      , map_keys(cast(correct_item as map(varchar, json)))[1] as problem_id
      , json_extract_scalar(
          cast(correct_item as map(varchar, json))[map_keys(cast(correct_item as map(varchar, json)))[1]],
          '$.correctness'
        ) as correctness
      , json_format(
          cast(answer_item as map(varchar, json))[map_keys(cast(answer_item as map(varchar, json)))[1]]
        ) as answers_json
    from indexed
  )

  select
    user_id
    , courserun_readable_id
    , studentmodule_id
    , studentmodule_state_data
    , coursestructure_block_id
    , studentmodule_problem_grade
    , studentmodule_problem_max_grade
    , studentmodule_updated_on
    , attempt
    , problem_id
    , correctness
    , answers_json
  from exploded
)
{% endmacro %}
