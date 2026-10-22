-- One row per extractor call: which scope of which run it read, with which extractor
-- version and model, and what it cost. Versions are rows, never overwrites, so two
-- extractors or models can be compared on the same scopes; is_current marks the ones
-- the rollups read and is_approved_version the ones of the run's allowlisted version
-- (int__courseware__extractions). The extraction key leaves the course header out,
-- so is_header_stale flags a run whose header changed since its extraction (the
-- extractor records the header on every read), for a deliberate re-extraction under
-- key_on_header.
with extractions as (
    select * from {{ ref('int__courseware__extractions') }}
)

, run_headers as (
    -- one header per read, so one per run
    select
        platform
        , courserun_readable_id
        , max(header_version) as header_version
    from {{ ref('int__courseware__latest_scope_inputs') }}
    group by platform, courserun_readable_id
)

, course_runs as (
    select * from {{ ref('dim_course_run') }}
    where is_current
)

, content as (
    select * from {{ ref('dim_course_content') }}
    where is_latest
)

select
    {{ dbt_utils.generate_surrogate_key(['extractions.extraction_key']) }} as courseware_extraction_pk
    , extractions.extraction_key
    , extractions.platform
    , extractions.courserun_readable_id
    , course_runs.courserun_pk as courserun_fk
    , content.content_block_pk as scope_block_fk
    , extractions.scope_id
    , extractions.scope_kind
    , extractions.scope_title
    , extractions.scope_position
    , extractions.scope_part
    , extractions.scope_part_count
    , extractions.extractor_id
    , extractions.prompt_version
    , extractions.schema_version
    , extractions.model_id
    , extractions.effort
    , extractions.input_text_checksum
    , extractions.header_version
    , run_headers.header_version as current_header_version
    , extractions.header_version != run_headers.header_version as is_header_stale
    , extractions.source_count
    , extractions.truncated_source_count
    , extractions.summary
    , extractions.notes_json
    , extractions.stop_reason
    , extractions.input_tokens
    , extractions.output_tokens
    , extractions.cache_read_input_tokens
    , extractions.cache_creation_input_tokens
    , extractions.extracted_at
    , extractions.is_approved_version
    , extractions.is_current
from extractions
left join run_headers
    on
        extractions.platform = run_headers.platform
        and extractions.courserun_readable_id = run_headers.courserun_readable_id
left join course_runs
    on
        extractions.platform = course_runs.platform
        and extractions.courserun_readable_id = course_runs.courserun_readable_id
left join content
    on
        extractions.platform = content.platform
        and extractions.courserun_readable_id = content.courserun_readable_id
        and extractions.scope_id = content.block_id
