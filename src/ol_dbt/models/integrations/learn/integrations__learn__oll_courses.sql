{#
  integrations__learn__oll_courses
  Exposes Open Learning Library courses for MIT Learn's ETL (Trino-pull or webhook).
  Contract: docs/learn_marts_contract.md

  OCW-origin rows are excluded in the staging model (offered_by = 'OCW') to avoid
  duplicating records already covered by integrations__learn__ocw_courses.

  OLL has no source-system last_modified timestamp; current_timestamp is used
  as a conservative fallback (documented in schema YAML below).
#}

with courses as (
    select * from {{ ref('stg__oll__google_sheets__courses') }}
)

select
    course_readable_id                                      as readable_id
    , course_title                                          as title
    , {{ cast_timestamp_to_iso8601('current_timestamp') }}  as last_modified
    , 'oll'                                                 as etl_source
    , description                                           as description
    , course_url                                            as url
    , course_image_url                                      as image_url
    , replace(course_topics_raw, '|', ', ')                 as topics
    , true                                                  as published
    , 'oll'                                                 as platform
    , 'course'                                              as resource_type
    -- run metadata: single run per course using run_id from the spreadsheet
    , concat(
        course_run_id,
        '|||true'
    )                                                       as runs
from courses
