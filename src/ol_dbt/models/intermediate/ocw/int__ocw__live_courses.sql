{#
  One row per OCW course published to the OCW live bucket, with the fields of
  its data.json that MIT Learn's OCW ETL reads (transform_course and
  transform_run in learning_resources/etl/ocw.py).

  int__ocw__courses is the same courses as ocw-studio holds them. data.json is
  what OCW published: its description is rendered HTML, its topics include the
  MIT Learn topics, and its paths are the published ones.

  A site renamed in ocw-studio is published under its new path and its old
  directory stays in the bucket, with a data.json that carries the same uids.
  MIT Learn keeps the newer of the two by data.json's S3 modification time,
  which the load does not record. ocw-studio's url_path is the path a site is
  published at now, so a directory no site has as its url_path is left out.

  A site keeps its url_path when it is unpublished. ocw-studio sets
  unpublish_status on the unpublish request, clears it when a live publish is
  requested, and leaves a site with any unpublish_status out of its own builds.
  The bucket load drops the course only on its next daily sweep after the
  prefix is removed, and never while the removal is pending or has errored, so
  the site is left out here from the first sync of ocw-studio's database after
  the request.
#}

with published_paths as (
    select distinct website_url_path
    from {{ ref('stg__ocw__studio__postgres__websites_website') }}
    where website_unpublish_status is null
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
        , {{ json_extract_scalar('course_data_json', "'$.extra_course_numbers'") }} as extra_course_numbers
        , nullif({{ json_extract_scalar('course_data_json', "'$.term'") }}, '') as course_term
        , nullif({{ json_extract_scalar('course_data_json', "'$.year'") }}, '') as course_year
        , nullif({{ json_extract_scalar('course_data_json', "'$.image_src'") }}, '') as course_image_src
        , cast({{ json_extract_value('course_data_json', "'$.hide_download'") }} as varchar) as hide_download_json
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
    -- Learn splits any non-empty string and strips each number, so "6.1, " and " "
    -- keep an empty number.
    , case
        when extra_course_numbers != ''
            then {{ array_transform(regexp_split('extra_course_numbers', "','"), 'number', python_strip('number')) }}
        else {{ empty_varchar_array() }}
    end as course_extra_course_numbers
    , course_term
    , course_year
    -- Learn iterates these fields, so an object gives its keys. Any other shape is
    -- empty, so one course cannot fail the build. Learn would take each character
    -- of a string as a tag or a department id, and fail the load on an unknown one.
    , coalesce(
        {{ json_extract_varchar_array('course_data_json', "'$.level'", lenient=true) }}
        , {{ json_object_key_array('course_data_json', "'$.level'") }}
        , {{ empty_varchar_array() }}
    ) as course_levels
    , coalesce(
        {{ json_extract_varchar_array('course_data_json', "'$.department_numbers'", lenient=true) }}
        , {{ json_object_key_array('course_data_json', "'$.department_numbers'") }}
        , {{ empty_varchar_array() }}
    ) as course_department_numbers
    , coalesce(
        {{ json_extract_varchar_array('course_data_json', "'$.learning_resource_types'", lenient=true) }}
        , {{ json_object_key_array('course_data_json', "'$.learning_resource_types'") }}
        , {{ empty_varchar_array() }}
    ) as course_learning_resource_types
    , {{ json_nested_array_distinct_values('course_data_json', "'$.topics'", lenient=true) }} as course_topics
    , {{ json_nested_array_distinct_values('course_data_json', "'$.mit_learn_topics'", lenient=true) }}
        as course_learn_topics
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
    -- Learn reads hide_download for its truth, so the string "false" hides it too.
    , coalesce(
        hide_download_json not in ('null', 'false', '""', '[]', '{}')
        and coalesce({{ try_cast('hide_download_json', 'double') }} != 0, true)
        , false
    ) as course_hides_download
    , course_retrieved_on
from parsed
