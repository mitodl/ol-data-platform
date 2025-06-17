{% macro generate_studentmodule_problem_events(studentmodule_table, studentmodulehistory_table, user_id_field) %}
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
      , cast(json_query(studentmodule_state_data, 'lax $.attempts' omit quotes) as int) as attempts
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
      , from_iso8601_timestamp_nanos(to_iso8601(studentmodule_created_on)) as studentmodule_created_on
      , cast(json_query(studentmodule_state_data, 'lax $.attempts' omit quotes) as int) as attempts
    from {{ studentmodulehistory_table }}
  )
  {% endif %}

  , base as (
    select
      sm.user_id
      , sm.courserun_readable_id
      , sm.studentmodule_id
      , sm.coursestructure_block_id
      {% if studentmodulehistory_table != 'null' %}
      , coalesce(smhe.studentmodule_created_on, sm.studentmodule_updated_on) as studentmodule_updated_on
      , coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data) as studentmodule_state_data
      , coalesce(smhe.studentmodule_problem_grade, sm.studentmodule_problem_grade) as studentmodule_problem_grade
      , coalesce(smhe.studentmodule_problem_max_grade, sm.studentmodule_problem_max_grade) as studentmodule_problem_max_grade
      , cast(json_parse(json_query(coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data), 'lax $.correct_map_history')) as array(json)) as correct_history
      , cast(json_parse(json_query(coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data), 'lax $.student_answers_history')) as array(json)) as answer_history
      , coalesce(smhe.attempts, 1) as recent_attempt_index
      from studentmodule sm
      left join studentmodulehistoryextended smhe
        on sm.studentmodule_id = smhe.studentmodule_id
      {% else %}
      , sm.studentmodule_updated_on
      , sm.studentmodule_state_data
      , sm.studentmodule_problem_grade
      , sm.studentmodule_problem_max_grade
      , cast(json_parse(json_query(sm.studentmodule_state_data, 'lax $.correct_map_history')) as array(json)) as correct_history
      , cast(json_parse(json_query(sm.studentmodule_state_data, 'lax $.student_answers_history')) as array(json)) as answer_history
      , coalesce(sm.attempts, 1) as recent_attempt_index
      from studentmodule sm
      {% endif %}
  )

  , processed as (
    select
      user_id
      , courserun_readable_id
      , studentmodule_id
      , studentmodule_state_data
      , coursestructure_block_id
      , studentmodule_problem_grade
      , studentmodule_problem_max_grade
      , studentmodule_updated_on
      , try(
          case
            when correct_history is not null
              and recent_attempt_index >= 1
              and cardinality(correct_history) >= recent_attempt_index
              then correct_history[recent_attempt_index]
            else null
          end
      ) as correct_item
      , try(
          case
            when answer_history is not null
              and recent_attempt_index >= 1
              and cardinality(answer_history) >= recent_attempt_index
              then answer_history[recent_attempt_index]
            else null
          end
      ) as answer_item
      , recent_attempt_index as attempt
    from base
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
      , cast(attempt as varchar) as attempt
      , case
          when correct_item is not null and cardinality(map_keys(cast(correct_item as map(varchar, json)))) >= 1
            then map_keys(cast(correct_item as map(varchar, json)))[1]
          else null
        end as problem_id
      , case
          when correct_item is not null and cardinality(map_keys(cast(correct_item as map(varchar, json)))) >= 1
            then json_extract_scalar(
              cast(correct_item as map(varchar, json))[map_keys(cast(correct_item as map(varchar, json)))[1]],
              '$.correctness'
            )
          else null
      end as correctness -- noqa: PRS
      , case
        when answer_item is not null and cardinality(map_keys(cast(answer_item as map(varchar, json)))) >= 1
          then json_format(
            cast(answer_item as map(varchar, json))[map_keys(cast(answer_item as map(varchar, json)))[1]]
          )
        else null
      end as answers_json -- noqa: PRS
    from processed
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
{% endmacro %}
