-- Each course run's latest read by the courseware extractor: one row per scope part
-- it read, extracted or skipped, with the input it computed (courseware_scope_input).
with scope_inputs as (
    select * from {{ source('courseware_intermediate', 'courseware_scope_input') }}
)

, latest_reads as (
    select
        platform
        , courserun_readable_id
        , max(observed_at) as observed_at
    from scope_inputs
    group by platform, courserun_readable_id
)

select
    scope_inputs.platform
    , scope_inputs.courserun_readable_id
    , scope_inputs.scope_id
    , scope_inputs.scope_part
    , scope_inputs.scope_part_count
    , scope_inputs.input_text_checksum
    , scope_inputs.header_version
    , scope_inputs.observed_at
from scope_inputs
inner join latest_reads
    on
        scope_inputs.platform = latest_reads.platform
        and scope_inputs.courserun_readable_id = latest_reads.courserun_readable_id
        and scope_inputs.observed_at = latest_reads.observed_at
