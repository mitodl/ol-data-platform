{#
  integrations__learn__mit_edx_programs
  Exposes MIT-authored edX.org programs for MIT Learn's ETL (Trino-pull or webhook).
  Contract: docs/learn_marts_contract.md

  Source: raw__edxorg__discovery__api__programs, which is pre-filtered by the
  dlt pipeline to active, MIT-authored, non-MicroMasters programs. MicroMasters
  are covered by integrations__learn__micromasters_programs instead.

  raw__edxorg__discovery__api__programs is loaded with write_disposition=
  "merge" (primary_key=uuid): a program that becomes inactive/withdrawn simply
  stops being yielded by the dlt pipeline, so merge leaves its existing row in
  place rather than deleting it. `published` is therefore NOT a constant --
  it's true only for programs whose _dlt_load_id matches the table's most
  recent load, i.e. programs the pipeline actually reconfirmed as active in
  the latest successful run. A program merge left behind from an older run
  gets published = false here instead of appearing live forever.

  The subjects and courses columns retain their JSON array format from the API
  because no cross-db macro is available to flatten JSON arrays to a
  comma-separated string. Consuming applications must parse the JSON.
#}

with programs as (
    select * from {{ ref('stg__edxorg__discovery__api__programs') }}
)

select
    program_uuid                                            as readable_id
    , program_title                                         as title
    , {{ cast_timestamp_to_iso8601('current_timestamp') }}  as last_modified
    , 'mit_edx'                                             as etl_source
    , program_description                                   as description
    , program_marketing_url                                 as url
    , program_image_url                                     as image_url
    -- subjects as JSON string: [{"name": "Engineering"}, ...]
    -- consumers should parse this JSON array to extract topic names
    , program_subjects_json                                 as topics_json
    -- course keys as JSON string: [{"key": "MITx/6.00.1x"}, ...]
    -- consumers should parse this JSON array to extract readable_ids
    , program_courses_json                                  as courses_json
    , (
        program_dlt_load_id = max(program_dlt_load_id) over ()
    )                                                        as published
    , 'edxorg'                                              as platform
    , 'program'                                             as resource_type
from programs
