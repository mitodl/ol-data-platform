with
    problems as (
        select
            content_block_pk,
            block_id,
            block_title,
            courserun_readable_id,
            nullif(json_query(block_metadata, 'lax $.markdown' omit quotes), 'null') as markdown,
            nullif(json_query(block_metadata, 'lax $.max_attempts' omit quotes), 'null') as max_attempts,
            nullif(json_query(block_metadata, 'lax $.start' omit quotes), 'null') as start_date,
            nullif(json_query(block_metadata, 'lax $.due' omit quotes), 'null') as due_date,
            nullif(json_query(block_metadata, 'lax $.weight' omit quotes), 'null') as weight,
            row_number() over (partition by block_id order by is_latest desc, retrieved_at desc) as row_num
        from {{ ref("dim_course_content") }}
        where block_category = 'problem'
    ),
    problem_events as (
        select distinct
            courserun_readable_id,
            json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id,
            json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name,
            json_extract(useractivity_event_object, '$.submission') as submission
        from {{ ref("stg__mitxonline__openedx__tracking_logs__user_activity") }}
        where
            courserun_readable_id is not null
            and useractivity_event_type = 'problem_check'
            and json_query(useractivity_event_object, 'lax $.submission' omit quotes) is not null

        union all

        select distinct
            courserun_readable_id,
            json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id,
            json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name,
            json_extract(useractivity_event_object, '$.submission') as submission
        from {{ ref("stg__mitxpro__openedx__tracking_logs__user_activity") }}
        where
            courserun_readable_id is not null
            and useractivity_event_type = 'problem_check'
            and json_query(useractivity_event_object, 'lax $.submission' omit quotes) is not null

        union all

        select distinct
            courserun_readable_id,
            json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id,
            json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name,
            json_extract(useractivity_event_object, '$.submission') as submission
        from {{ ref("stg__mitxresidential__openedx__tracking_logs__user_activity") }}
        where
            courserun_readable_id is not null
            and useractivity_event_type = 'problem_check'
            and json_query(useractivity_event_object, 'lax $.submission' omit quotes) is not null

        union all

        select distinct
            courserun_readable_id,
            json_query(useractivity_event_object, 'lax $.problem_id' omit quotes) as problem_block_id,
            json_query(useractivity_context_object, 'lax $.module.display_name' omit quotes) as problem_name,
            json_extract(useractivity_event_object, '$.submission') as submission
        from {{ ref("stg__edxorg__s3__tracking_logs__user_activity") }}
        where
            courserun_readable_id is not null
            and useractivity_event_type = 'problem_check'
            and json_query(useractivity_event_object, 'lax $.submission' omit quotes) is not null
    ),
    problem_type_metadata as (
        select
            problem_events.courserun_readable_id,
            problem_events.problem_block_id,
            arbitrary(problem_events.problem_name) as problem_name,
            array_agg(distinct json_extract_scalar(t.submission_data, '$.response_type')) as problem_types
        from problem_events, unnest(cast(problem_events.submission as map<varchar, json>)) as t(key, submission_data)
        where
            json_extract_scalar(t.submission_data, '$.response_type') is not null
            and json_extract_scalar(t.submission_data, '$.response_type') <> ''
        group by problem_events.courserun_readable_id, problem_events.problem_block_id
    ),
    combined as (
        select
            problems.content_block_pk as content_block_fk,
            problems.markdown,
            problems.max_attempts,
            problems.start_date,
            problems.due_date,
            problems.weight,
            problems.block_id as problem_block_pk,
            problems.courserun_readable_id,
            problems.block_title as problem_name,
            problem_type_metadata.problem_types
        from problems
        left join problem_type_metadata on problems.block_id = problem_type_metadata.problem_block_id
        where problems.row_num = 1
    )

select
    problem_block_pk,
    content_block_fk,
    courserun_readable_id,
    problem_name,
    markdown,
    max_attempts,
    start_date,
    due_date,
    weight,
    problem_types
from combined
