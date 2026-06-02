{#
  integrations__learn__mitpe_programs
  Exposes MIT Professional Education programs for MIT Learn's ETL (Trino-pull or webhook).
  Contract: docs/learn_marts_contract.md

  MIT PE has no source-system last_modified timestamp, so current_timestamp is used
  as a conservative fallback (documented in schema YAML below).
#}

with programs as (
    select * from {{ ref('stg__mitpe__api__programs') }}
)

select
    program_readable_id                                     as readable_id
    , program_title                                         as title
    , {{ cast_timestamp_to_iso8601('current_timestamp') }}  as last_modified
    , 'mitpe'                                               as etl_source
    , program_description                                   as description
    , concat('https://professional.mit.edu', program_url)  as url
    , case
        when program_image_src is not null and program_image_src != ''
            then concat('https://professional.mit.edu', program_image_src)
    end                                                     as image_url
    , program_image_alt                                     as image_alt
    , replace(program_topics_raw, '|', ', ')                as topics
    , true                                                  as published
    , 'mitpe'                                               as platform
    , 'program'                                             as resource_type
from programs
