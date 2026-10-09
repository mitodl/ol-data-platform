-- The tfact_feedback post_hook must leave no stored turn that is gone upstream, for
-- every source that still has rows upstream; a stale turn can duplicate a turn_index.
-- Any row means the hook did not run, so this fails at one row, not the default ten.
{{ config(error_if='>0') }}

select feedback.feedback_pk
from {{ ref('tfact_feedback') }} as feedback
where
    feedback.feedback_pk not in (
        select {{ dbt_utils.generate_surrogate_key(['source_slug', 'source_record_ref']) }}
        from {{ ref('int__feedback__unioned') }}
    )
    and feedback.feedback_source_fk in (
        select distinct {{ dbt_utils.generate_surrogate_key(['source_slug']) }}
        from {{ ref('int__feedback__unioned') }}
    )
