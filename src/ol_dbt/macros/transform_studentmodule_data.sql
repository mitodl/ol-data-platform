{% macro generate_studentmodule_problem_events(studentmodule_table, studentmodulehistory_table, user_id_field, platform='mitxonline', watermark_expr=none) %}
  -- Narrow projection from studentmodule: only the fields needed to annotate
  -- each history record. No watermark filter here — we must include ALL problems
  -- that have new submissions, not just problems first created after the watermark.
  -- (Filtering studentmodule by creation time would silently drop re-submissions
  -- on older problems, since a student can submit attempt N+1 on a problem created
  -- months ago.)
  with studentmodule as (
    select
      studentmodule_id
      , courserun_readable_id
      , coursestructure_block_id
      , {{ user_id_field }} as user_id
    from {{ studentmodule_table }}
    where coursestructure_block_category = 'problem'
  )

  -- One row per submission event. Each time a student clicks "Check", OpenEdX
  -- writes a history record with the state at that moment. The state_data JSON
  -- contains:
  --   attempts      integer  — which submission number this is (1-based)
  --   seed          integer  — random seed for this student/problem variant
  --   correct_map   object   — per-part correctness for THIS submission only
  --   student_answers object — per-part answers for THIS submission
  --   correct_map_history array — growing accumulation of all prior submissions;
  --                               redundant across rows — reconstructible by
  --                               ordering on event_timestamp, so excluded here
  --   input_state   object   — UI widget rendering state; not analytically useful
  --
  -- studentmodule_created_on is timestamp(6) in the Iceberg staging table;
  -- cast directly to timestamp with time zone (session TZ = UTC).
  , studentmodulehistory as (
    select
      studentmodulehistoryextended_id
      , studentmodule_id
      , cast(studentmodule_problem_grade as varchar) as grade
      , cast(studentmodule_problem_max_grade as varchar) as max_grade
      , cast(studentmodule_created_on as timestamp(6) with time zone) as event_timestamp
      , {{ json_query_string('studentmodule_state_data', "'$.attempts'") }} as attempt
      , {{ json_query_string('studentmodule_state_data', "'$.seed'") }} as seed
      -- correct_map is the per-part correctness for this submission only (~400 bytes).
      -- correct_map_history (the growing accumulation) is intentionally excluded.
      , cast({{ json_extract_value('studentmodule_state_data', "'$.correct_map'") }} as varchar) as correct_map
      -- student_answers is the per-part answers for this submission. Keys are opaque
      -- problem-part IDs matching those in correct_map; values are strings or arrays
      -- (multiple-choice). Not collapsible to fixed columns due to sparse/variable keys.
      , cast({{ json_extract_value('studentmodule_state_data', "'$.student_answers'") }} as varchar) as answers
    from {{ studentmodulehistory_table }}
    where
      -- Records without an attempt number are initial module-creation events, not
      -- student submissions. Exclude them.
      {{ json_query_string('studentmodule_state_data', "'$.attempts'") }} is not null
      {% if watermark_expr %}
        and (
          {{ watermark_expr }} is null
          or cast(studentmodule_created_on as timestamp(6) with time zone) > {{ watermark_expr }}
        )
      {% elif is_incremental() %}
        and (
          cast(studentmodule_created_on as timestamp(6) with time zone) > (
            select max(event_timestamp) from {{ this }}
            where platform = '{{ platform }}'
          )
          or not exists (select 1 from {{ this }} where platform = '{{ platform }}')
        )
      {% endif %}
  )

  -- Join history events to studentmodule for course/block/user context.
  -- Inner join: history records without a matching studentmodule are excluded
  -- (orphaned rows with no owning problem module).
  , history_joined as (
    select
      sm.user_id
      , sm.courserun_readable_id
      , sm.studentmodule_id
      , sm.coursestructure_block_id
      , h.studentmodulehistoryextended_id
      , h.grade
      , h.max_grade
      , h.event_timestamp
      , h.attempt
      , h.seed
      , h.correct_map
      , h.answers
      , if(
          h.grade is not null
          and h.max_grade is not null
          and h.grade = h.max_grade
          , 'correct'
          , 'incorrect'
        ) as success
    from studentmodule as sm
    inner join studentmodulehistory as h
      on sm.studentmodule_id = h.studentmodule_id
  )

  select
    user_id
    , courserun_readable_id
    , studentmodule_id
    , coursestructure_block_id
    , studentmodulehistoryextended_id
    , grade
    , max_grade
    , event_timestamp
    , attempt
    , seed
    , correct_map
    , answers
    , success
  from history_joined
{% endmacro %}
