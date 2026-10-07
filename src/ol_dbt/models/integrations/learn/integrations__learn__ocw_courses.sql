{#
  integrations__learn__ocw_courses
  The OCW courses in the OCW live bucket, with the fields MIT Learn's OCW ETL
  (learning_resources/etl/ocw.py) builds a course and its one run from.
  Contract: docs/learn_marts_contract.md

  Not reproduced: Learn passes the description through nh3, parses instructors
  with parse_instructors, and maps ocw_topics to its own topics for a course
  with no topics of its own. The description, instructors and ocw_topics here
  are as OCW published them.
#}

with courses as (
    select * from {{ ref('int__ocw__live_courses') }}
)

select
    -- Django's slugify of the term: "January IAP" -> "january-iap".
    course_primary_course_number
    || coalesce(
        '+' || {{ regexp_replace_all(
            regexp_replace_all("lower(trim(course_term))", "'[^a-z0-9_\\s-]'", "''"), "'[-\\s]+'", "'-'"
        ) }}
        , ''
    )
    || coalesce('_' || course_year, '') as readable_id
    , course_title as title
    , course_retrieved_on as last_modified
    , 'ocw' as etl_source
    , course_description_html as description
    , '{{ var("ocw_production_url") }}courses/' || course_slug || '/' as url
    , case
        when course_image_src is not null
            then {{ url_join("'" ~ var("ocw_production_url").rstrip("/") ~ "'", 'course_image_src') }}
    end as image_url
    , course_image_alt as image_alt
    , course_image_description as image_description
    , true as published
    , 'ocw' as platform
    , courserun_readable_id as run_id
    , 'courses/' || course_slug as slug
    , course_term as term
    , {{ try_cast('course_year', 'integer') }} as year
    , course_levels as level
    , course_primary_course_number as course_number
    , course_extra_course_numbers as extra_course_numbers
    , course_department_numbers as departments
    , course_learn_topics as topics
    , course_topics as ocw_topics
    , course_learning_resource_types as content_tags
    , course_instructors_json as instructors
    , course_hides_download as hide_download
from courses
-- Learn skips a course with neither uid.
where courserun_readable_id is not null
