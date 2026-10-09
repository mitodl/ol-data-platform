{#
  One row per OCW course published to the OCW live bucket, with the fields of
  its data.json that MIT Learn's OCW ETL reads (transform_course and
  transform_run in learning_resources/etl/ocw.py).

  int__ocw__courses is the same courses as ocw-studio holds them. data.json is
  what OCW published: its description is rendered HTML, its topics include the
  MIT Learn topics, and its paths are the published ones.

  A site renamed in ocw-studio is published under its new path and its old
  directory stays in the bucket, with a data.json that carries the same uids.
  MIT Learn keeps the newer of the two by the files' S3 modification times,
  which the load does not record. ocw-studio's url_path is the path a site is
  published at now, so a directory no site has as its url_path is left out.
#}

with published_paths as (
    select distinct website_url_path
    from {{ ref('stg__ocw__studio__postgres__websites_website') }}
    where website_url_path is not null
)

, content as (
    select course_content.*
    from {{ ref('stg__ocw__s3__course_content') }} as course_content
    inner join published_paths
        on concat('courses/', course_content.course_slug) = published_paths.website_url_path
    where course_content.coursecontent_kind = 'course'
)

, parsed as (
    select
        course_slug
        , course_data_json
        , coursecontent_retrieved_on as course_retrieved_on
        , nullif(course_site_uid, '') as course_site_uid
        , nullif(course_legacy_uid, '') as course_legacy_uid
        , {{ json_extract_scalar('course_data_json', "'$.course_title'") }} as course_title
        , {{ json_extract_scalar('course_data_json', "'$.course_description_html'") }} as course_description_html
        , nullif({{ json_extract_scalar('course_data_json', "'$.primary_course_number'") }}, '')
            as course_primary_course_number
        , nullif(trim({{ json_extract_scalar('course_data_json', "'$.extra_course_numbers'") }}), '')
            as extra_course_numbers
        , nullif({{ json_extract_scalar('course_data_json', "'$.term'") }}, '') as course_term
        , nullif({{ json_extract_scalar('course_data_json', "'$.year'") }}, '') as course_year
        , nullif({{ json_extract_scalar('course_data_json', "'$.image_src'") }}, '') as course_image_src
    from content
)

select
    course_slug
    -- Learn's run_id.
    , replace(coalesce(course_legacy_uid, course_site_uid), '-', '') as courserun_readable_id
    , course_site_uid
    , course_legacy_uid
    , course_title
    , course_description_html
    , course_primary_course_number
    , case
        when extra_course_numbers is not null then {{ regexp_split('extra_course_numbers', "'\\s*,\\s*'") }}
        else {{ empty_varchar_array() }}
    end as course_extra_course_numbers
    , course_term
    , course_year
    , coalesce({{ json_extract_varchar_array('course_data_json', "'$.level'") }}, {{ empty_varchar_array() }})
        as course_levels
    , coalesce(
        {{ json_extract_varchar_array('course_data_json', "'$.department_numbers'") }}, {{ empty_varchar_array() }}
    ) as course_department_numbers
    , coalesce(
        {{ json_extract_varchar_array('course_data_json', "'$.learning_resource_types'") }}
        , {{ empty_varchar_array() }}
    ) as course_learning_resource_types
    , {{ json_nested_array_distinct_values('course_data_json', "'$.topics'") }} as course_topics
    , {{ json_nested_array_distinct_values('course_data_json', "'$.mit_learn_topics'") }} as course_learn_topics
    -- A JSON null reads as the text 'null', here and for the image alt text.
    , coalesce(
        nullif({{ json_array_string('course_data_json', "'$.instructors'") }}, 'null'), '[]'
    ) as course_instructors_json
    , course_image_src
    , nullif(
        {{ json_query_string('course_data_json', "'$.course_image_metadata.image_metadata.\"image-alt\"'") }}
        , 'null'
    ) as course_image_alt
    , {{ json_extract_scalar('course_data_json', "'$.course_image_metadata.description'") }}
        as course_image_description
    , coalesce({{ json_extract_scalar('course_data_json', "'$.hide_download'") }} = 'true', false)
        as course_hides_download
    , course_retrieved_on
from parsed
