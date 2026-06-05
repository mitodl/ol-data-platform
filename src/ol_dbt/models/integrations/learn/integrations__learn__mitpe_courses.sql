{#
  integrations__learn__mitpe_courses
  Exposes MIT Professional Education courses for MIT Learn's ETL (Trino-pull or webhook).
  Contract: docs/learn_marts_contract.md

  MIT PE has no source-system last_modified timestamp, so current_timestamp is used
  as a conservative fallback (documented in schema YAML below).
#}

with courses as (
    select * from {{ ref('stg__mitpe__api__courses') }}
)

select
    course_readable_id                                      as readable_id
    , course_title                                          as title
    , {{ cast_timestamp_to_iso8601('current_timestamp') }}  as last_modified
    , 'mitpe'                                               as etl_source
    , course_description                                    as description
    , concat('https://professional.mit.edu', course_url)   as url
    , case
        when course_image_src is not null and course_image_src != ''
            then concat('https://professional.mit.edu', course_image_src)
    end                                                     as image_url
    , course_image_alt                                      as image_alt
    , replace(course_topics_raw, '|', ', ')                 as topics
    , true                                                  as published
    , 'mitpe'                                               as platform
    , 'course'                                              as resource_type
from courses
