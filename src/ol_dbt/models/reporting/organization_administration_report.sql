with enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, certificate_org_data as (
    select
        platform
        , course_title
        , courserun_readable_id
        , user_email
        , cast(substring(courseruncertificate_created_on, 1, 10) as date) as certificate_created_date
    from enrollment_detail
    group by
        platform
        , course_title
        , courserun_readable_id
        , user_email
        , cast(substring(courseruncertificate_created_on, 1, 10) as date)
)

select
    platform
    , course_title
    , courserun_readable_id
    , certificate_created_date
    , user_email
    , case when certificate_created_date is not null then 1 else 0 end as certificate_count
from certificate_org_data
