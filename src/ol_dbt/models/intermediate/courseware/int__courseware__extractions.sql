{#
    Every claim extraction, and whether it is current: the one each part of a scope
    is read from.

    A reading of a scope is how one run split it into parts, recorded on each of
    that run's extractions as scope_part_checksums (every part's
    input_text_checksum, in order). A reading is complete when, under one extractor
    version and model, every part has an extraction whose input_text_checksum is
    that part's, whichever run wrote it: a part whose text did not change keeps its
    extraction key, so a later run skips it rather than writing it again. The
    current reading is the newest complete one, and each of its parts reads from
    its newest matching extraction.

    So a run that extracts only some parts of a new reading (the rest failed, and
    are retried on the next run) changes nothing until the reading is complete. The
    older reading stays current whole, rather than losing the failed parts' claims
    or mixing parts from two different splits. A new extractor version or model
    starts a new reading the same way, and a reading in fewer parts leaves no older
    part current.

    A reading is dated by the newest extraction that records it. Text that changes
    and then changes back writes nothing new (every key is stored), so the later
    reading stays current until the scope is read again.

    A scope the run's sources no longer have (a sequential since read as context,
    say) has no current extraction, or its old claims would be counted beside the
    new.
#}
with extractions as (
    select * from {{ source('courseware_intermediate', 'courseware_claim_extraction') }}
)

, current_scopes as (
    select distinct
        platform
        , courserun_readable_id
        , scope_id
    from {{ ref('int__courseware__extraction_sources') }}
)

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
        , max(extracted_at) as read_at
    from extractions
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
)

, current_readings as (
    select * from (
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
            , row_number() over (
                partition by platform, courserun_readable_id, scope_id
                order by read_at desc, scope_part_checksums ASC
            ) as reading_rank
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
            , read_at
        having count(distinct scope_part) = scope_part_count
    ) as complete_readings
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
    , extractions.scope_position
    , extractions.scope_part
    , extractions.scope_part_count
    , extractions.input_text_checksum
    , extractions.scope_part_checksums
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
    , (
        current_extractions.extraction_key is not null
        and current_scopes.scope_id is not null
    ) as is_current
from extractions
left join current_extractions
    on extractions.extraction_key = current_extractions.extraction_key
left join current_scopes
    on
        extractions.platform = current_scopes.platform
        and extractions.courserun_readable_id = current_scopes.courserun_readable_id
        and extractions.scope_id = current_scopes.scope_id
