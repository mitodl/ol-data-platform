{% macro generate_studentmodule_problem_events(studentmodule_table, studentmodulehistory_table, user_id_field) %}
    with
        studentmodule as (
            select
                studentmodule_id,
                courserun_readable_id,
                coursestructure_block_id,
                coursestructure_block_category,
                {{ user_id_field }} as user_id,
                studentmodule_state_data,
                cast(studentmodule_problem_grade as varchar) as grade,
                cast(studentmodule_problem_max_grade as varchar) as max_grade,
                from_iso8601_timestamp_nanos(studentmodule_created_on) as studentmodule_created_on,
                from_iso8601_timestamp_nanos(studentmodule_updated_on) as studentmodule_updated_on,
                cast(json_query(studentmodule_state_data, 'lax $.attempts' omit quotes) as int) as attempt
            from {{ studentmodule_table }}
            where coursestructure_block_category = 'problem'
        ),
        studentmodulehistoryextended as (
            select
                studentmodulehistoryextended_id,
                studentmodule_id,
                studentmodule_state_data,
                cast(studentmodule_problem_grade as varchar) as grade,
                cast(studentmodule_problem_max_grade as varchar) as max_grade,
                from_iso8601_timestamp_nanos(to_iso8601(studentmodule_created_on)) as studentmodule_created_on,
                cast(json_query(studentmodule_state_data, 'lax $.attempts' omit quotes) as int) as attempt
            from {{ studentmodulehistory_table }}
        ),
        -- Pull out arrays from the state data
        -- Exclude rows without an attempt number, as these are not valid problem events
        -- Records from historyextended that join to studentmodule with fallback/default logic
        history_joined as (
            select
                sm.user_id,
                sm.courserun_readable_id,
                sm.studentmodule_id,
                sm.coursestructure_block_id,
                coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data) as studentmodule_state_data,
                coalesce(smhe.grade, sm.grade) as grade,
                coalesce(smhe.max_grade, sm.max_grade) as max_grade,
                coalesce(smhe.studentmodule_created_on, sm.studentmodule_updated_on) as event_timestamp,
                cast(smhe.attempt as varchar) as attempt,
                if(
                    coalesce(smhe.grade, sm.grade) is not null
                    and coalesce(smhe.max_grade, sm.max_grade) is not null
                    and coalesce(smhe.grade, sm.grade) = coalesce(smhe.max_grade, sm.max_grade),
                    'correct',
                    'incorrect'
                ) as success,
                cast(
                    json_query(
                        coalesce(smhe.studentmodule_state_data, sm.studentmodule_state_data), 'lax $.student_answers'
                    ) as varchar
                ) as answers
            from studentmodule sm
            left join studentmodulehistoryextended smhe on sm.studentmodule_id = smhe.studentmodule_id
            where smhe.attempt is not null
        )

    select
        user_id,
        courserun_readable_id,
        studentmodule_id,
        coursestructure_block_id,
        studentmodule_state_data,
        grade,
        max_grade,
        event_timestamp,
        attempt,
        success,
        answers
    from history_joined
{% endmacro %}
