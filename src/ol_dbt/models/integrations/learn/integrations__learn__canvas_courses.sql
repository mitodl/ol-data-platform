{#
  integrations__learn__canvas_courses
  One row per Canvas course with an export in the landing zone. ingest_canvas_course
  writes a Canvas course and its content files from one archive, so moving Canvas
  off it moves the (canvas, course) and (canvas, content_file) ownership keys
  together, and this model is the course half. Fields are what
  run_for_canvas_archive writes.

  Each course has the one run Learn creates for it. Learn keeps a Canvas resource
  unpublished and in test mode, which is for its Sync task to set, so neither is
  here. Learn's fallbacks are reproduced: a course with no title in its settings
  is "canvas course <id>", and a missing domain or Canvas id reads "None" in the
  URL, as Python formats it.
#}
with courses as (select * from {{ ref("stg__canvas__s3__course_content") }} where coursecontent_kind = 'course')

select
    'canvas' as etl_source,
    course_id,
    replace({{ canvas_run_readable_id("course_id", "course_code") }}, '+canvas', '') as readable_id,
    {{ canvas_run_readable_id("course_id", "course_code") }} as run_id,
    course_code,
    coalesce(course_title, concat('canvas course ', course_id)) as title,
    concat(
        'https://',
        coalesce(course_canvas_domain, 'None'),
        '/courses/',
        coalesce(course_canvas_id, 'None'),
        '/'
    ) as url,
    course_start_on as start_date,
    course_end_on as end_date,
    coursecontent_retrieved_on as last_modified
from courses
