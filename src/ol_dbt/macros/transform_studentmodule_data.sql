{% macro generate_studentmodule_problem_events(studentmodule_table, studentmodulehistory_table, user_id_field, platform='mitxonline', watermark_expr=none) %}
  with studentmodule as (
    select
      studentmodule_id
      , courserun_readable_id
      , coursestructure_block_id
      , coursestructure_block_category
      , {{ user_id_field }} as user_id
      , studentmodule_state_data
      , cast(studentmodule_problem_grade as varchar) as grade
      , cast(studentmodule_problem_max_grade as varchar) as max_grade
      , from_iso8601_timestamp_nanos(studentmodule_created_on) as studentmodule_created_on
      , from_iso8601_timestamp_nanos(studentmodule_updated_on) as studentmodule_updated_on
      , json_query(studentmodule_state_data, 'lax $.attempts' omit quotes) as attempt
    from {{ studentmodule_table }}
    where coursestructure_block_category = 'problem'
    {% if watermark_expr %}
      and from_iso8601_timestamp_nanos(studentmodule_created_on) > {{ watermark_expr }}
    {% elif is_incremental() %}
      and from_iso8601_timestamp_nanos(studentmodule_created_on) > (
          select max(event_timestamp) from {{ this }}
          where platform = '{{ platform }}'
      )
    {% endif %}

  )

  -- Read the raw history table and extract the attempt number per row.
  , studentmodulehistory_raw as (
    select
      studentmodule_id
      , studentmodule_state_data
      , cast(studentmodule_problem_grade as varchar) as grade
      , cast(studentmodule_problem_max_grade as varchar) as max_grade
      -- studentmodule_created_on is already timestamp(6) in the Iceberg staging table.
      -- The previous from_iso8601_timestamp_nanos(to_iso8601(col)) round-trip was a
      -- wasteful string serialization for 89M+ residential history rows. Cast directly.
      , cast(studentmodule_created_on as timestamp(6) with time zone) as studentmodule_created_on
      , json_query(studentmodule_state_data, 'lax $.attempts' omit quotes) as attempt
    from {{ studentmodulehistory_table }}
    {% if watermark_expr %}
      where cast(studentmodule_created_on as timestamp(6) with time zone) > {{ watermark_expr }}
    {% elif is_incremental() %}
      where cast(studentmodule_created_on as timestamp(6) with time zone) > (
          select max(event_timestamp) from {{ this }}
            where platform = '{{ platform }}'
      )
    {% endif %}

  )

  -- Pre-aggregate to one row per (studentmodule_id, attempt), keeping the
  -- LAST (most recent) history entry for each attempt.
  --
  -- The history table stores one row per submission event within an attempt.
  -- The state_data JSON contains a `correct_map_history` array that grows with
  -- every submission — the LAST record therefore contains the complete view of
  -- all submissions for that attempt, the final grade, and the final student
  -- answers. Using max_by here:
  --   1. Collapses the fan-out before joins (eliminates the expensive global
  --      shuffle in the downstream TopNRanking dedup).
  --   2. Produces the semantically correct event_json: the final state with
  --      the full submission history, not just the first (incomplete) state.
  -- max(studentmodule_created_on) is stored as the timestamp so the incremental
  -- watermark in the fact table correctly advances to the latest processed event.
  , studentmodulehistoryextended as (
    select
      studentmodule_id
      , max_by(studentmodule_state_data, studentmodule_created_on) as studentmodule_state_data
      , max_by(grade, studentmodule_created_on) as grade
      , max_by(max_grade, studentmodule_created_on) as max_grade
      , max(studentmodule_created_on) as studentmodule_created_on
      , attempt
    from studentmodulehistory_raw
    where attempt is not null
    group by studentmodule_id, attempt
  )

  -- Pull out arrays from the state data
  -- Exclude rows without an attempt number, as these are not valid problem events
  -- Records from historyextended that join to studentmodule with fallback/default logic
  , history_joined as (
    select
      sm.user_id
      , sm.courserun_readable_id
      , sm.studentmodule_id
      , sm.coursestructure_block_id
      , coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data) as studentmodule_state_data
      , coalesce(smhe.grade, sm.grade) as grade
      , coalesce(smhe.max_grade, sm.max_grade) as max_grade
      , coalesce(smhe.studentmodule_created_on, sm.studentmodule_updated_on) as event_timestamp
      , smhe.attempt as attempt
      , if(
            coalesce(smhe.grade, sm.grade) is not null
            and coalesce(smhe.max_grade, sm.max_grade) is not null
            and coalesce(smhe.grade, sm.grade) = coalesce(smhe.max_grade, sm.max_grade)
            , 'correct'
            , 'incorrect'
        ) as success
      , cast(
            json_query(
                coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data),
                'lax $.student_answers')
            as varchar
        ) as answers
      -- inner join: only rows with a history entry that has an attempt are valid events
      from studentmodule sm
      inner join studentmodulehistoryextended smhe
        on sm.studentmodule_id = smhe.studentmodule_id
  )

  select
    user_id
    , courserun_readable_id
    , studentmodule_id
    , coursestructure_block_id
    , studentmodule_state_data
    , grade
    , max_grade
    , event_timestamp
    , attempt
    , success
    , answers
  from history_joined
{% endmacro %}
