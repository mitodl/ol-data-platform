{#
  integrations__learn__content_file_units
  One row per unit of an MITx Online or xPRO course run, as MIT Learn's
  OlxArchive.units builds them from the run's export (mit-learn#4095), for
  SyncOpenEdXContentFilesTask to load with load_course_units after the run's
  content files. Scoped like integrations__learn__content_files, by
  (etl_source, run_readable_id).

  content_files lists each unit's ContentFile keys with their relation, in lesson
  order, as JSON [[key, relation], ...]. It is the unit as Learn builds it, before
  load_course_units drops the links to files Learn has no published ContentFile
  for, drops a unit (other than the context) left with none, and numbers what is
  left; unit_order is the unit's place in that list.

  Units come from the same export as the content files' blocks, or the run has no
  rows here, as a run whose units could not be built has none. Learn keeps a
  run's units when it reads none for it.
#}

with units as (
    select * from {{ ref('stg__openedx__s3__course_units') }}
    where unit_source_system in ('mitxonline', 'xpro')
)

, block_versions as (
    select distinct
        courserun_readable_id
        , coursestructure_xml_source_system as source_system
        , coursestructure_xml_archive_version as course_xml_version
    from {{ ref('stg__openedx__s3__course_xml_blocks') }}
    where coursestructure_xml_source_system in ('mitxonline', 'xpro')
)

select
    units.unit_source_system as etl_source
    , units.courserun_readable_id as run_readable_id
    , units.unit_external_id as external_id
    , units.unit_kind as kind
    , units.unit_title as title
    -- get_jump_to_url; the context unit has none
    , case
        when units.unit_url_name != ''
            then concat(
                {{ openedx_lms_root_url('units.unit_source_system') }}
                , '/courses/', units.courserun_readable_id, '/jump_to_id/', units.unit_url_name
            )
        else ''
    end as url
    , units.unit_order
    , units.unit_content_files as content_files
from units
inner join block_versions
    on units.courserun_readable_id = block_versions.courserun_readable_id
    and units.unit_source_system = block_versions.source_system
    and units.unit_course_xml_version = block_versions.course_xml_version
