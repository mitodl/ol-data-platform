{#
    One row per course run in int__courseware__extraction_sources that
    courseware_extraction_allowlist approves, with a checksum of every field the
    extractor's scopes are built from and the extractor version the run is approved
    for. The courseware extraction sensor (dg_projects/ml) reports the checksum as
    the run's data version while the approved version is the deployed extractor's,
    so a run is extracted again only when its text changes or it is promoted to a
    new version. A run takes its own allowlist row, else its course's (through
    dim_course_run.course_fk, so a run with no dim_course course matches only run and
    platform rows), else its platform's. Extraction itself still skips every
    scope whose extraction_key is stored, so a checksum change costs model calls only
    for the scopes that actually changed. The key also hashes the model and effort,
    which the allowlist does not approve, so the first extraction of a run after a
    model change calls the model for every scope. md5 because it is all change
    detection needs and md5_hex gives the same digest on every engine.
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

, allowlist as (
    select * from {{ ref('courseware_extraction_allowlist') }}
)

, courseruns as (
    select
        dim_course_run.platform
        , dim_course_run.courserun_readable_id
        , dim_course.course_readable_id
    from {{ ref('dim_course_run') }} as dim_course_run
    inner join {{ ref('dim_course') }} as dim_course
        on dim_course_run.course_fk = dim_course.course_pk
    where dim_course_run.is_current
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

, run_checksums as (
    select
        platform
        , courserun_readable_id
        , count(*) as source_count
        , {{ md5_hex(array_join(
            'array_agg(source_checksum order by scope_position, scope_id, source_position)', ','
        )) }} as text_checksum
    from source_checksums
    group by platform, courserun_readable_id
)

, allowlist_matches as (
    select
        run_checksums.platform
        , run_checksums.courserun_readable_id
        , run_checksums.source_count
        , run_checksums.text_checksum
        , allowlist.extractor_id
        , allowlist.prompt_version
        , allowlist.schema_version
        , row_number() over (
            partition by run_checksums.platform, run_checksums.courserun_readable_id
            order by
                case
                    when allowlist.courserun_readable_id is not null then 1
                    when allowlist.course_readable_id is not null then 2
                    else 3
                end
        ) as match_rank
    from run_checksums
    left join courseruns
        on
            run_checksums.platform = courseruns.platform
            and run_checksums.courserun_readable_id = courseruns.courserun_readable_id
    inner join allowlist
        on
            run_checksums.platform = allowlist.platform
            and (
                allowlist.courserun_readable_id = run_checksums.courserun_readable_id
                or (
                    allowlist.courserun_readable_id is null
                    and allowlist.course_readable_id = courseruns.course_readable_id
                )
                or (
                    allowlist.courserun_readable_id is null
                    and allowlist.course_readable_id is null
                )
            )
)

select
    platform
    , courserun_readable_id
    , source_count
    , text_checksum
    , extractor_id
    , prompt_version
    , schema_version
from allowlist_matches
where match_rank = 1
