{#
    Every claim extraction, and whether it is current: the one each part of a scope
    is read from.

    A reading of a scope is how one run split it into parts, recorded on each of
    that run's extractions as scope_part_checksums (every part's
    input_text_checksum, in order). A reading is complete when, under one extractor
    version and model, every part has an extraction whose input_text_checksum is
    that part's, whichever run wrote it: a part whose text did not change keeps its
    extraction key, so a later run skips it rather than writing it again. For the
    same reason the latest read's split is a reading even when no extraction records
    it: its parts can all be stored under other splits (a,b and c,d, then a,d).

    A two-stage (v2) extraction's key also covers the course header its context
    stage read, so for v2 the header_version is part of the reading too: after a
    header edit every part is placed in the new context, and the new reading is
    complete only once all of them are. A v1 key left the header out (or hashed it
    into the whole message without recording it), so a v1 reading spans headers.

    The current reading is the first complete one by:

    1. the extractor version (extractor, prompt and schema) that
       int__courseware__extraction_allowlist approves for the run, so a trial run or
       a comparison under another version never becomes current by finishing last;
    2. matching the scope's input text at the extractor's latest read of the run
       (int__courseware__latest_scope_inputs);
    3. matching the course header at that read (v2);
    4. the newest.

    Each of its parts reads from its newest matching extraction.

    The approved version ranks first rather than filtering the rest out, so after a
    run is promoted its previous reading stays current until the new version's is
    complete. A run the allowlist does not cover (OCW, a run var('courseware_courseruns')
    adds locally, or one taken off the allowlist) is ranked on the other two alone.
    The allowlist does not approve a model or effort, so two models' readings of
    the approved version are ranked on the other two as well.

    So a run that extracts only some parts of a new reading (the rest failed, and
    are retried on the next run) changes nothing until the reading is complete. The
    older reading stays current whole, rather than losing the failed parts' claims
    or mixing parts from two different splits. A new extractor version or model
    starts a new reading the same way, and a reading in fewer parts leaves no older
    part current.

    Text or a header that changes and then changes back writes nothing new (every
    key is stored), but the extractor records each read's input, so the earlier
    reading is current again. With no reading matching the latest read (the new text's parts
    are not all extracted yet, or the run has not been read since inputs were
    recorded), the newest complete reading is current. A reading is dated by the
    newest extraction that records it.

    A scope the run's sources no longer have (a sequential since read as context,
    say) has no current extraction, or its old claims would be counted beside the
    new. That needs the run's sources in this build: for a run with none (off the
    allowlist, or left out by var('courseware_courseruns')), only a unit scope whose
    sequential is gone from the run's latest course structure (dim_course_content)
    is dropped, and runs with no Open edX structure (OCW) keep every scope.

    scope_position is the scope's position in the course as it is now, from
    int__courseware__extraction_sources, since the extraction key leaves position
    out and reordering a course moves a scope without re-extracting it. The position
    stored at extraction time is kept for a scope the sources do not list.
#}
{%- set stored = source('courseware_intermediate', 'courseware_claim_extraction') %}
-- The v2 columns exist once the extraction asset first writes a v2 row; a table
-- only v1 wrote lacks them.
with stored_extractions as (
    select
        *
        {{- missing_columns_as_nulls(stored, {
            'header_version': dbt.type_string(),
            'text_extraction_key': dbt.type_string(),
            'text_input_digest': dbt.type_string(),
            'context_input_digest': dbt.type_string(),
        }) }}
    from {{ stored }}
)

, extractions as (
    select
        *
        -- the header a reading is under, where the key covers it (v2)
        , case when text_extraction_key is not null then header_version end as reading_header_version
    from stored_extractions
)

, current_scopes as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , scope_position
    from {{ ref('int__courseware__extraction_sources') }}
)

, source_runs as (
    select distinct
        platform
        , courserun_readable_id
    from current_scopes
)

, approved_versions as (
    select
        platform
        , courserun_readable_id
        , extractor_id
        , prompt_version
        , schema_version
    from {{ ref('int__courseware__extraction_allowlist') }}
)

, structure as (
    select
        platform
        , courserun_readable_id
        , block_id
        , block_category
    from {{ ref('dim_course_content') }}
    where is_latest
)

, structure_runs as (
    select distinct
        platform
        , courserun_readable_id
    from structure
)

, structure_sequentials as (
    select distinct
        platform
        , courserun_readable_id
        , block_id as scope_id
    from structure
    where block_category = 'sequential'
)

, latest_scope_inputs as (
    select * from {{ ref('int__courseware__latest_scope_inputs') }}
)

-- one header per read: every part of a run's read records the same one
, latest_headers as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
        , header_version
    from latest_scope_inputs
)

-- the latest read's split of each scope, as scope_part_checksums
, latest_splits as (
    select
        platform
        , courserun_readable_id
        , scope_id
        , scope_part_count
        , {{ array_join('array_agg(input_text_checksum order by scope_part)', ',') }} as scope_part_checksums
    from latest_scope_inputs
    group by platform, courserun_readable_id, scope_id, scope_part_count
)

-- Every split an extraction records, and the latest read's split under each version,
-- model and header that has any of its parts. Nothing records a split recombined
-- from parts of stored ones (after readings a,b and c,d, a read of a,d finds both
-- keys stored and writes no row), so the latest read's split is built here.
, readings as (
    select
        platform
        , courserun_readable_id
        , scope_id
        , extractor_id
        , prompt_version
        , schema_version
        , model_id
        , effort
        , scope_part_count
        , scope_part_checksums
        , reading_header_version
        , max(read_at) as read_at
    from (
        select
            platform
            , courserun_readable_id
            , scope_id
            , extractor_id
            , prompt_version
            , schema_version
            , model_id
            , effort
            , scope_part_count
            , scope_part_checksums
            , reading_header_version
            , extracted_at as read_at
        from extractions

        union all

        select
            extractions.platform
            , extractions.courserun_readable_id
            , extractions.scope_id
            , extractions.extractor_id
            , extractions.prompt_version
            , extractions.schema_version
            , extractions.model_id
            , extractions.effort
            , latest_splits.scope_part_count
            , latest_splits.scope_part_checksums
            , extractions.reading_header_version
            , extractions.extracted_at as read_at
        from extractions
        inner join latest_scope_inputs
            on
                extractions.platform = latest_scope_inputs.platform
                and extractions.courserun_readable_id = latest_scope_inputs.courserun_readable_id
                and extractions.scope_id = latest_scope_inputs.scope_id
                and extractions.scope_part = latest_scope_inputs.scope_part
                and extractions.scope_part_count = latest_scope_inputs.scope_part_count
                and extractions.input_text_checksum = latest_scope_inputs.input_text_checksum
        inner join latest_splits
            on
                latest_scope_inputs.platform = latest_splits.platform
                and latest_scope_inputs.courserun_readable_id = latest_splits.courserun_readable_id
                and latest_scope_inputs.scope_id = latest_splits.scope_id
    ) as recorded_splits
    group by
        platform
        , courserun_readable_id
        , scope_id
        , extractor_id
        , prompt_version
        , schema_version
        , model_id
        , effort
        , scope_part_count
        , scope_part_checksums
        , reading_header_version
)

-- the extractions that hold each part of each reading
, reading_parts as (
    select
        readings.platform
        , readings.courserun_readable_id
        , readings.scope_id
        , readings.extractor_id
        , readings.prompt_version
        , readings.schema_version
        , readings.model_id
        , readings.effort
        , readings.scope_part_count
        , readings.scope_part_checksums
        , readings.reading_header_version
        , readings.read_at
        , extractions.extraction_key
        , extractions.scope_part
        , extractions.extracted_at
    from readings
    inner join extractions
        on
            readings.platform = extractions.platform
            and readings.courserun_readable_id = extractions.courserun_readable_id
            and readings.scope_id = extractions.scope_id
            and readings.extractor_id = extractions.extractor_id
            and readings.prompt_version = extractions.prompt_version
            and readings.schema_version = extractions.schema_version
            and readings.model_id = extractions.model_id
            and readings.effort = extractions.effort
            and readings.scope_part_count = extractions.scope_part_count
            and split_part(readings.scope_part_checksums, ',', extractions.scope_part)
            = extractions.input_text_checksum
            and coalesce(readings.reading_header_version, '')
            = coalesce(extractions.reading_header_version, '')
)

, current_readings as (
    select * from (
        select
            complete_readings.*
            , row_number() over (
                partition by
                    complete_readings.platform
                    , complete_readings.courserun_readable_id
                    , complete_readings.scope_id
                order by
                    case when approved_versions.courserun_readable_id is not null then 0 else 1 end
                    , case when latest_splits.scope_id is not null then 0 else 1 end
                    , case
                        when
                            complete_readings.reading_header_version is null
                            or complete_readings.reading_header_version = latest_headers.header_version
                            then 0
                        else 1
                    end
                    , complete_readings.read_at desc
                    , complete_readings.scope_part_checksums asc
            ) as reading_rank
        from (
            select
                platform
                , courserun_readable_id
                , scope_id
                , extractor_id
                , prompt_version
                , schema_version
                , model_id
                , effort
                , scope_part_count
                , scope_part_checksums
                , reading_header_version
                , read_at
            from reading_parts
            group by
                platform
                , courserun_readable_id
                , scope_id
                , extractor_id
                , prompt_version
                , schema_version
                , model_id
                , effort
                , scope_part_count
                , scope_part_checksums
                , reading_header_version
                , read_at
            having count(distinct scope_part) = scope_part_count
        ) as complete_readings
        left join latest_splits
            on
                complete_readings.platform = latest_splits.platform
                and complete_readings.courserun_readable_id = latest_splits.courserun_readable_id
                and complete_readings.scope_id = latest_splits.scope_id
                and complete_readings.scope_part_count = latest_splits.scope_part_count
                and complete_readings.scope_part_checksums = latest_splits.scope_part_checksums
        left join latest_headers
            on
                complete_readings.platform = latest_headers.platform
                and complete_readings.courserun_readable_id = latest_headers.courserun_readable_id
                and complete_readings.scope_id = latest_headers.scope_id
        left join approved_versions
            on
                complete_readings.platform = approved_versions.platform
                and complete_readings.courserun_readable_id = approved_versions.courserun_readable_id
                and complete_readings.extractor_id = approved_versions.extractor_id
                and complete_readings.prompt_version = approved_versions.prompt_version
                and complete_readings.schema_version = approved_versions.schema_version
    ) as ranked_readings
    where reading_rank = 1
)

, current_extractions as (
    select extraction_key from (
        select
            reading_parts.extraction_key
            , row_number() over (
                partition by reading_parts.platform, reading_parts.courserun_readable_id
                , reading_parts.scope_id, reading_parts.scope_part
                order by reading_parts.extracted_at desc
            ) as part_rank
        from reading_parts
        inner join current_readings
            on
                reading_parts.platform = current_readings.platform
                and reading_parts.courserun_readable_id = current_readings.courserun_readable_id
                and reading_parts.scope_id = current_readings.scope_id
                and reading_parts.extractor_id = current_readings.extractor_id
                and reading_parts.prompt_version = current_readings.prompt_version
                and reading_parts.schema_version = current_readings.schema_version
                and reading_parts.model_id = current_readings.model_id
                and reading_parts.effort = current_readings.effort
                and reading_parts.scope_part_count = current_readings.scope_part_count
                and reading_parts.scope_part_checksums = current_readings.scope_part_checksums
                and coalesce(reading_parts.reading_header_version, '')
                = coalesce(current_readings.reading_header_version, '')
    ) as ranked_parts
    where part_rank = 1
)

select
    extractions.extraction_key
    , extractions.extractor_id
    , extractions.prompt_version
    , extractions.schema_version
    , extractions.model_id
    , extractions.effort
    , extractions.platform
    , extractions.courserun_readable_id
    , extractions.scope_id
    , extractions.scope_kind
    , extractions.scope_title
    , coalesce(current_scopes.scope_position, extractions.scope_position) as scope_position
    , extractions.scope_part
    , extractions.scope_part_count
    , extractions.input_text_checksum
    , extractions.scope_part_checksums
    , extractions.header_version
    , extractions.text_extraction_key
    , extractions.text_input_digest
    , extractions.context_input_digest
    , extractions.source_count
    , extractions.truncated_source_count
    , extractions.sources_json
    , extractions.summary
    , extractions.notes_json
    , extractions.stop_reason
    , extractions.input_tokens
    , extractions.output_tokens
    , extractions.cache_read_input_tokens
    , extractions.cache_creation_input_tokens
    , extractions.extracted_at
    -- null when the allowlist does not cover the run
    , case
        when run_approvals.courserun_readable_id is not null
            then
                extractions.extractor_id = run_approvals.extractor_id
                and extractions.prompt_version = run_approvals.prompt_version
                and extractions.schema_version = run_approvals.schema_version
    end as is_approved_version
    , coalesce(
        extractions.scope_part_count = latest_scope_inputs.scope_part_count
        and extractions.input_text_checksum = latest_scope_inputs.input_text_checksum
        and (
            extractions.reading_header_version is null
            or extractions.reading_header_version = latest_scope_inputs.header_version
        )
        , false
    ) as matches_current_input
    , (
        current_extractions.extraction_key is not null
        and (
            current_scopes.scope_id is not null
            or (
                source_runs.courserun_readable_id is null
                and not (
                    extractions.scope_kind = 'unit'
                    and structure_runs.courserun_readable_id is not null
                    and structure_sequentials.scope_id is null
                )
            )
        )
    ) as is_current
from extractions
left join current_extractions
    on extractions.extraction_key = current_extractions.extraction_key
left join current_scopes
    on
        extractions.platform = current_scopes.platform
        and extractions.courserun_readable_id = current_scopes.courserun_readable_id
        and extractions.scope_id = current_scopes.scope_id
left join latest_scope_inputs
    on
        extractions.platform = latest_scope_inputs.platform
        and extractions.courserun_readable_id = latest_scope_inputs.courserun_readable_id
        and extractions.scope_id = latest_scope_inputs.scope_id
        and extractions.scope_part = latest_scope_inputs.scope_part
left join source_runs
    on
        extractions.platform = source_runs.platform
        and extractions.courserun_readable_id = source_runs.courserun_readable_id
left join approved_versions as run_approvals
    on
        extractions.platform = run_approvals.platform
        and extractions.courserun_readable_id = run_approvals.courserun_readable_id
left join structure_runs
    on
        extractions.platform = structure_runs.platform
        and extractions.courserun_readable_id = structure_runs.courserun_readable_id
left join structure_sequentials
    on
        extractions.platform = structure_sequentials.platform
        and extractions.courserun_readable_id = structure_sequentials.courserun_readable_id
        and extractions.scope_id = structure_sequentials.scope_id
