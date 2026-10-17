{#
    One row per course run in int__courseware__extraction_sources, with a checksum of
    every field the extractor's scopes are built from. The courseware extraction
    sensor (dg_projects/ml) reports it as the run's data version, so a run is
    extracted again only when its text changes. Extraction itself still skips every
    scope whose extraction_key is stored, so a checksum change costs model calls only
    for the scopes that actually changed. md5 because it is all change detection needs
    and md5_hex gives the same digest on every engine.
#}
{% set source_fields = [
    "coalesce(course_title, '')",
    "scope_id",
    "scope_kind",
    "coalesce(scope_title, '')",
    "cast(scope_position as varchar)",
    "cast(source_position as varchar)",
    "source_ref",
    "coalesce(source_kind, '')",
    "coalesce(source_title, '')",
    "source_text",
] %}

with sources as (
    select * from {{ ref('int__courseware__extraction_sources') }}
)

, source_checksums as (
    select
        platform
        , courserun_readable_id
        , scope_position
        , scope_id
        , source_position
        , {{ md5_hex("concat_ws('|', " ~ source_fields | join(', ') ~ ")") }} as source_checksum
    from sources
)

select
    platform
    , courserun_readable_id
    , count(*) as source_count
    , {{ md5_hex(array_join(
        'array_agg(source_checksum order by scope_position, scope_id, source_position)', ','
    )) }} as text_checksum
from source_checksums
group by platform, courserun_readable_id
