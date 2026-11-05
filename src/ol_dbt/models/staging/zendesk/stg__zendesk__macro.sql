with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__thirdparty__zendesk_support__macros') }}
)

{{ deduplicate_raw_table(raw_table='raw__thirdparty__zendesk_support__macros', partition_columns = 'id') }}
-- The reply text is in the comment_value_html action, or comment_value in older macros
, actions as (
    select
        most_recent_source.id
        , max(case when {{ json_strict_value('t.json_str', "'$.field'") }} = 'comment_value_html'
            then {{ json_strict_value('t.json_str', "'$.value'") }} end) as comment_value_html
        , max(case when {{ json_strict_value('t.json_str', "'$.field'") }} = 'comment_value'
            then {{ json_strict_value('t.json_str', "'$.value'") }} end) as comment_value
        , max(case when {{ json_strict_value('t.json_str', "'$.field'") }} = 'comment_mode_is_public'
            then {{ json_strict_value('t.json_str', "'$.value'") }} end) as comment_mode_is_public
    from most_recent_source
    cross join unnest(most_recent_source.actions) as t(json_str)
    group by most_recent_source.id
)

, cleaned as (
    select
        most_recent_source.id as macro_id
        , most_recent_source.url as macro_api_url
        , most_recent_source.title as macro_title
        , most_recent_source.raw_title as macro_raw_title
        , most_recent_source.description as macro_description
        , most_recent_source.active as macro_is_active
        , most_recent_source."default" as macro_is_default
        , most_recent_source.position as macro_position
        , most_recent_source.actions as macro_actions
        , nullif({{ json_query_string('most_recent_source.restriction', "'$.type'") }}, 'null')
            as macro_restriction_type
        , {{ json_extract_value('most_recent_source.restriction', "'$.ids'") }} as macro_restriction_group_ids
        , actions.comment_value_html as macro_comment_html
        , actions.comment_value as macro_comment_text
        , case actions.comment_mode_is_public
            when 'true' then true
            when 'false' then false
        end as macro_comment_is_public
        , {{ cast_timestamp_to_iso8601('most_recent_source.created_at') }} as macro_created_at
        , {{ cast_timestamp_to_iso8601('most_recent_source.updated_at') }} as macro_updated_at
    from most_recent_source
    left join actions on most_recent_source.id = actions.id
)

select * from cleaned
