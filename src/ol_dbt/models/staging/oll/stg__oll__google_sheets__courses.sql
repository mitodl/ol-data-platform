with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__oll__google_sheets__courses') }}
)

select
    oll_course as course_number
    -- The sheet's readable_id is the edX run key, e.g. MITx+0.501x+2T2019
    , readable_id as courserun_readable_id
    , title as course_title
    , url as course_url
    , description as course_description
    , course_image_url_flat as course_image_url
    , offered_by as course_offered_by
    , published = 'YES' as course_is_published
    , level as course_level
    , language as course_language
    , mi_tx_o_primary_child as course_primary_topic
    , mi_tx_o_adopted_secondary_child as course_secondary_topic
    , instructor_1
    , instructor_2
    , instructor_3
    , instructor_4
    , instructor_5
    , instructor_6
    , duration as course_duration_weeks
    , student_effort as course_weekly_hours
from source
where oll_course is not null and oll_course != ''
