with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitpe__api__courses') }}
)

select
    uuid                                        as course_uuid
    , title                                     as course_title
    , url                                       as course_url
    , description                               as course_description
    , image__src                                as course_image_src
    , image__alt                                as course_image_alt
    , topics                                    as course_topics_raw
    , learning_format                           as course_learning_format
    , resource_type                             as course_resource_type
    , course_certificate                        as course_certificates_raw
    -- programs only: pipe-separated titles of the program's courses
    , courses                                   as program_course_titles_raw
    -- pipe-separated and positionally aligned: the Nth run id goes with the Nth
    -- start/end/enrollment-end date
    , run__readable_id                          as course_run_ids_raw
    , start_date                                as course_run_start_dates_raw
    , end_date                                  as course_run_end_dates_raw
    , enrollment_end_date                       as course_run_enrollment_end_dates_raw
    , price                                     as course_price_raw
    , lead_instructors                          as course_lead_instructors_raw
    , instructors                               as course_instructors_raw
    , location                                  as course_location
    , duration                                  as course_duration
from source
where uuid is not null
