with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__oll__google_sheets__courses') }}
)

select
    -- Derive course-level readable_id: MITx+{course_code}, matching the
    -- convention in learning_resources/etl/oll.py.
    concat('MITx+', oll_course)                as course_readable_id
    , title                                     as course_title
    , url                                       as course_url
    , description                               as course_description
    , course_image_url_flat                     as course_image_url
    -- readable_id in the CSV is the run-level identifier (includes term, e.g.
    -- MITx+0.501x+2T2019). Fall back to course-level id if absent.
    , coalesce(
        nullif(trim(readable_id), ''),
        concat('MITx+', oll_course)
    )                                           as course_run_id
    , duration                                  as course_duration_weeks_raw
    -- Combine three subject columns into a pipe-separated string so the
    -- integration layer can split them consistently.
    , nullif(
        concat_ws(
            '|',
            nullif(trim(ed_x_subject_1), ''),
            nullif(trim(ed_x_subject_2), ''),
            nullif(trim(ed_x_subject_3), '')
        ),
        ''
    )                                           as course_topics_raw
    , offered_by                                as course_offered_by
    -- The source CSV has no semester/year columns; set to null so the
    -- de-dup ordering in the integration model degrades gracefully.
    , cast(null as varchar)                     as course_semester
    , cast(null as varchar)                     as course_year
from source
-- Skip OCW-origin rows — those courses are already covered by the OCW ETL path
where offered_by != 'OCW'
  and oll_course is not null
  and oll_course != ''
