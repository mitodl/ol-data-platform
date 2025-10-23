with enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, user_course_roles as (
    select * from {{ ref('int__combined__user_course_roles') }}
)

, org_field as (
    select 
        distinct courserun_readable_id 
        , organization
    from user_course_roles
)

, certificate_org_data as (
    select 
        distinct platform
        , course_title
        , courserun_readable_id
        , user_email
        , cast(substring(courseruncertificate_created_on, 1, 10) as date) as certificate_created_date
    from enrollment_detail
)

select
    certificate_org_data.platform
    , certificate_org_data.course_title
    , certificate_org_data.courserun_readable_id
    , certificate_org_data.certificate_created_date
    , certificate_org_data.user_email
    , case when certificate_org_data.certificate_created_date is not null then 1 else 0 end as certificate_count
    , org_field.organization
from certificate_org_data
left join org_field
    on certificate_org_data.courserun_readable_id = org_field.courserun_readable_id