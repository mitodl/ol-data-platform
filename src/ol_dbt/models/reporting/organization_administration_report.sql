with enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, user_course_roles as (
    select * from {{ ref('int__combined__user_course_roles') }}
)

, ai__chatbot as (
    select * from {{ ref('int__learn_ai__chatbot') }}
)

, ai__tutorbot as (
    select * from {{ ref('int__learn_ai__tutorbot') }}
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

, enroll_data as (
    select
        distinct platform
        , course_title
        , courserun_readable_id
        , user_email
    from enrollment_detail
)

, chatbot_data as (
    select 
        distinct user_email
        , cast(substring(chatsession_created_on, 1, 10) as date) as activity_date
        , courserun_readable_id
        , 1 as chatbot_used_count
        , 0 as certificate_count
    from ai__chatbot
    where user_email is not null

    union

    select 
        distinct user_email
        , cast(substring(chatsession_created_on, 1, 10) as date) as activity_date
        , courserun_readable_id
        , 1 as chatbot_used_count
        , 0 as certificate_count
    from ai__tutorbot
    where user_email is not null

    union

    select 
        distinct user_email
        , certificate_created_date as activity_date
        , courserun_readable_id
        , 0 as chatbot_used_count
        , 1 as certificate_count
    from certificate_org_data
    where certificate_created_date is not null

)

, activity_day_data as (
    select 
        user_email
        , activity_date
        , courserun_readable_id
        , max(chatbot_used_count) as chatbot_used_count
        , max(certificate_count) as certificate_count
    from chatbot_data
    group by
        user_email
        , activity_date
        , courserun_readable_id
)


select
    enroll_data.platform
    , enroll_data.course_title
    , enroll_data.courserun_readable_id
    , enroll_data.user_email
    , org_field.organization
    , activity_day_data.activity_date
    , activity_day_data.chatbot_used_count
    , activity_day_data.certificate_count
from enroll_data
left join org_field
    on enroll_data.courserun_readable_id = org_field.courserun_readable_id
left join activity_day_data
    on 
        enroll_data.user_email = activity_day_data.user_email
        and enroll_data.courserun_readable_id = activity_day_data.courserun_readable_id