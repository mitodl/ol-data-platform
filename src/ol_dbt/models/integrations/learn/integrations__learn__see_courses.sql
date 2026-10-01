{#
  integrations__learn__see_courses
  Sloan Executive Education courses for MIT Learn: every course MIT Learn's legacy
  Sloan ETL would load, with the course-level fields transform_course set. A course
  missing here is one MIT Learn treats as unpublished. Runs are in
  integrations__learn__see_runs.
  Contract: docs/learn_marts_contract.md
#}

select
    readable_id
    , title
    , {{ cast_timestamp_to_iso8601('updated_on') }} as last_modified
    , description
    , url
    , image_url
    , image_alt
    , topics
    , delivery
    , pace
    , format
    , continuing_ed_credits
    , true as certification
    , 'professional' as certification_type
    , true as professional
    , true as published
    , 'see' as etl_source
    , 'see' as platform
    , 'see' as offered_by
    , 'course' as resource_type
from {{ ref('int__see__courses') }}
