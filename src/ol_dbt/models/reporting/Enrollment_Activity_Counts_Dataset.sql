with enroll_dtl as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, combined__orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, combined_result as (
    select 
        enroll_dtl.user_email
        , enroll_dtl.user_gender
        , enroll_dtl.user_country_code
        , enroll_dtl.user_highest_education
        , enroll_dtl.courserun_is_current
        , enroll_dtl.platform
        , enroll_dtl.course_readable_id
        , enroll_dtl.courserun_readable_id
        , substring(enroll_dtl.courserunenrollment_created_on, 1, 10) as activity_date
        , 0 as certificate_count
        , case when enroll_dtl.courserunenrollment_enrollment_status = 'unenrolled' then 0 else 1 end
            as enrollment_count
        , case when enroll_dtl.courserunenrollment_enrollment_status = 'unenrolled' then 1 else 0 end
            as unenrolled_count
        , max(case when enroll_dtl.courserunenrollment_enrollment_mode = 'audit' 
            and (enroll_dtl.courserunenrollment_enrollment_status is null 
            or enroll_dtl.courserunenrollment_enrollment_status <> 'unenrolled')
            then 1 else 0 end) as audit_count
        , 0 as verified_count
    from enroll_dtl
    group by
        enroll_dtl.user_email
        , enroll_dtl.user_gender
        , enroll_dtl.user_country_code
        , enroll_dtl.user_highest_education
        , enroll_dtl.courserun_is_current
        , enroll_dtl.platform
        , enroll_dtl.course_readable_id
        , enroll_dtl.courserun_readable_id
        , substring(enroll_dtl.courserunenrollment_created_on, 1, 10)
        , case when enroll_dtl.courserunenrollment_enrollment_status = 'unenrolled' then 0 else 1 end
        , case when enroll_dtl.courserunenrollment_enrollment_status = 'unenrolled' then 1 else 0 end

    union 

    select  
        en_dtl.user_email
        , en_dtl.user_gender
        , en_dtl.user_country_code
        , en_dtl.user_highest_education
        , en_dtl.courserun_is_current
        , en_dtl.platform
        , en_dtl.course_readable_id
        , en_dtl.courserun_readable_id
        , substring(en_dtl.courseruncertificate_created_on, 1, 10) as activity_date
        , max(case when en_dtl.courseruncertificate_created_on is not null then 1 else 0 end) as certificate_count
        , 0 as enrollment_count
        , 0 as unenrolled_count
        , 0 as audit_count
        , 0 as verified_count
    from enroll_dtl as en_dtl
    where en_dtl.courseruncertificate_created_on is not null
        and (en_dtl.courserunenrollment_enrollment_status is null 
        or en_dtl.courserunenrollment_enrollment_status <> 'unenrolled')
    group by
        en_dtl.user_email
        , en_dtl.user_gender
        , en_dtl.user_country_code
        , en_dtl.user_highest_education
        , en_dtl.courserun_is_current
        , en_dtl.platform
        , en_dtl.course_readable_id
        , en_dtl.courserun_readable_id
        , substring(en_dtl.courseruncertificate_created_on, 1, 10)

    union 

    select
        a.user_email
        , a.user_gender
        , a.user_country_code
        , a.user_highest_education
        , a.courserun_is_current
        , a.platform
        , a.course_readable_id
        , a.courserun_readable_id
        , substring(coalesce(combined__orders.receipt_payment_timestamp
        , combined__orders.order_created_on, a.courserunenrollment_created_on), 1, 10) as activity_date
        , 0 as certificate_count
        , 0 as enrollment_count
        , 0 as unenrolled_count
        , 0 as audit_count
        , 1 as verified_count
    from enroll_dtl as a
    left join combined__orders
        on a.order_id = combined__orders.order_Id
        and a.line_id = combined__orders.line_id
        and a.platform = combined__orders.platform
    where a.courserunenrollment_enrollment_mode = 'verified'
        and (courserunenrollment_enrollment_status is null 
        or courserunenrollment_enrollment_status <> 'unenrolled')
    group by
        a.user_email
        , a.user_gender
        , a.user_country_code
        , a.user_highest_education
        , a.courserun_is_current
        , a.platform
        , a.course_readable_id
        , a.courserun_readable_id
        , substring(coalesce(combined__orders.receipt_payment_timestamp
        , combined__orders.order_created_on, a.courserunenrollment_created_on), 1, 10)
)

select 
    user_email
    , user_gender
    , user_country_code
    , user_highest_education
    , courserun_is_current
    , platform
    , course_readable_id
    , courserun_readable_id
    , activity_date
    , sum(certificate_count) as certificate_count
    , sum(enrollment_count) as enrollment_count
    , sum(unenrolled_count) as unenrolled_count
    , sum(audit_count) as audit_count
    , sum(verified_count) as verified_count
from combined_result
group by
    user_email
    , user_gender
    , user_country_code
    , user_highest_education
    , courserun_is_current
    , platform
    , course_readable_id
    , courserun_readable_id
    , activity_date