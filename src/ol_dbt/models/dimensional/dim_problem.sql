with problems as (
    select
        content_block_pk
        , platform
        , block_id
        , block_title
        , courserun_readable_id
        , nullif({{ json_query_string('block_metadata', "'$.markdown'") }}, 'null') as markdown
        , nullif({{ json_query_string('block_metadata', "'$.max_attempts'") }}, 'null') as max_attempts
        , nullif({{ json_query_string('block_metadata', "'$.start'") }}, 'null') as start_date
        , nullif({{ json_query_string('block_metadata', "'$.due'") }}, 'null') as due_date
        , nullif({{ json_query_string('block_metadata', "'$.weight'") }}, 'null') as weight
        , row_number() over (
            partition by platform, block_id
            order by is_latest desc, retrieved_at desc
        ) as row_num
    from {{ ref('dim_course_content') }}
    where block_category = 'problem'
)

, problem_events as (
    select distinct
        courserun_readable_id
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , json_extract(useractivity_event_object, '$.submission') as submission
    from (
        select
            courserun_readable_id
            , useractivity_event_type
            , cast(useractivity_event_object as varchar) as useractivity_event_object
            , cast(useractivity_context_object as varchar) as useractivity_context_object
        from {{ ref('stg__mitxonline__openedx__tracking_logs__user_activity') }}
    ) as mitxonline_logs
    where
        courserun_readable_id is not null
        and useractivity_event_type = 'problem_check'
        and {{ json_query_string('useractivity_event_object', "'$.submission'") }} is not null
        and {{ json_is_object("json_extract(useractivity_event_object, '$.submission')") }}

    union all

    select distinct
        courserun_readable_id
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , json_extract(useractivity_event_object, '$.submission') as submission
    from (
        select
            courserun_readable_id
            , useractivity_event_type
            , cast(useractivity_event_object as varchar) as useractivity_event_object
            , cast(useractivity_context_object as varchar) as useractivity_context_object
        from {{ ref('stg__mitxpro__openedx__tracking_logs__user_activity') }}
    ) as mitxpro_logs
    where
        courserun_readable_id is not null
        and useractivity_event_type = 'problem_check'
        and {{ json_query_string('useractivity_event_object', "'$.submission'") }} is not null
        and {{ json_is_object("json_extract(useractivity_event_object, '$.submission')") }}

    union all

    select distinct
        courserun_readable_id
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , json_extract(useractivity_event_object, '$.submission') as submission
    from (
        select
            courserun_readable_id
            , useractivity_event_type
            , cast(useractivity_event_object as varchar) as useractivity_event_object
            , cast(useractivity_context_object as varchar) as useractivity_context_object
        from {{ ref('stg__mitxresidential__openedx__tracking_logs__user_activity') }}
    ) as mitxresidential_logs
    where
        courserun_readable_id is not null
        and useractivity_event_type = 'problem_check'
        and {{ json_query_string('useractivity_event_object', "'$.submission'") }} is not null
        and {{ json_is_object("json_extract(useractivity_event_object, '$.submission')") }}

    union all

    select distinct
        courserun_readable_id
        , {{ json_query_string('useractivity_event_object', "'$.problem_id'") }} as problem_block_id
        , {{ json_query_string('useractivity_context_object', "'$.module.display_name'") }} as problem_name
        , json_extract(useractivity_event_object, '$.submission') as submission
    from (
        select
            courserun_readable_id
            , useractivity_event_type
            , cast(useractivity_event_object as varchar) as useractivity_event_object
            , cast(useractivity_context_object as varchar) as useractivity_context_object
        from {{ ref('stg__edxorg__s3__tracking_logs__user_activity') }}
    ) as edxorg_logs
    where
        courserun_readable_id is not null
        and useractivity_event_type = 'problem_check'
        and {{ json_query_string('useractivity_event_object', "'$.submission'") }} is not null
        and {{ json_is_object("json_extract(useractivity_event_object, '$.submission')") }}
)

, problem_type_metadata as (
    select
        problem_events.courserun_readable_id
        , problem_events.problem_block_id
        , arbitrary(problem_events.problem_name) as problem_name
        , array_agg(
            distinct
            {{ json_query_string('t.submission_data', "'$.response_type'") }}
        ) as problem_types
    from problem_events
    cross join {{ unnest_json_map('problem_events.submission', 't', 'key', 'submission_data') }}
    where
        {{ json_query_string('t.submission_data', "'$.response_type'") }} is not null
        and {{ json_query_string('t.submission_data', "'$.response_type'") }} <> ''
    group by problem_events.courserun_readable_id, problem_events.problem_block_id
)

, combined as (
    select
        problems.content_block_pk as content_block_fk
        , problems.platform
        , problems.markdown
        , problems.max_attempts
        , problems.start_date
        , problems.due_date
        , problems.weight
        , problems.block_id as problem_block_pk
        , problems.courserun_readable_id
        , problems.block_title as problem_name
        , problem_type_metadata.problem_types
    from problems
    left join problem_type_metadata
        on problems.block_id = problem_type_metadata.problem_block_id
         and problems.courserun_readable_id = problem_type_metadata.courserun_readable_id
    where problems.row_num = 1
)

select
    problem_block_pk
    , content_block_fk
    , platform
    , courserun_readable_id
    , problem_name
    , markdown
    , max_attempts
    , start_date
    , due_date
    , weight
    , problem_types
from combined
