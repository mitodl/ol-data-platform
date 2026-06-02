with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__oll__google_sheets__courses') }}
)

select
    -- Derive readable_id: MITx+{course_code} following the same convention as
    -- learning_resources/etl/oll.py. OCW-origin rows are excluded below.
    concat('MITx+', oll_course)                as course_readable_id
    , title                                     as course_title
    , url                                       as course_url
    , description                               as course_description
    , course_image_url_flat                     as course_image_url
    , coalesce(run_id, concat('MITx+', oll_course))
                                                as course_run_id
    , duration                                  as course_duration_weeks_raw
    , subjects                                  as course_topics_raw
    , offered_by                                as course_offered_by
    , semester                                  as course_semester
    , year                                      as course_year
from source
-- Skip OCW-origin rows — those courses are already covered by the OCW ETL path
where offered_by != 'OCW'
  and oll_course is not null
  and oll_course != ''
