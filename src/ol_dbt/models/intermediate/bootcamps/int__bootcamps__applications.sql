with applications as (
    select * from {{ ref('stg__bootcamps__app__postgres__applications_courserun_application') }}
)

, runs as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_courserun') }}
)

, application_steps as (
    select * from {{ ref('stg__bootcamps__app__postgres__applications_applicationstep') }}
)

, application_last_steps as (
    select
        course_id
        , max(applicationstep_step_order) as last_step
    from application_steps
    group by course_id
)

, courserun_applicationstep as (
    select * from {{ ref('stg__bootcamps__app__postgres__applications_courserun_applicationstep') }}
)

, applicationstep_submissions as (
    select * from {{ ref('stg__bootcamps__app__postgres__applications_applicationstep_submission') }}
)

, personal_prices as (
    select * from {{ ref('stg__bootcamps__app__postgres__courses_personalprice') }}
)

, installments as (
    select
        courserun_id
        , sum(installment_amount) as list_price
    from {{ ref('stg__bootcamps__app__postgres__courses_installment') }}
    group by courserun_id
)

, fulfilled_orders as (
    select
        application_id
        , sum(line_price) as total_paid
    from {{ ref('int__bootcamps__ecommerce_order') }}
    where order_state = 'fulfilled'
    group by application_id
)


, courserun_last_steps as (
    select
        courserun_applicationstep.courserun_applicationstep_id
        , courserun_applicationstep.courserun_id
        , application_steps.applicationstep_step_order
    from
        courserun_applicationstep
    inner join application_steps on courserun_applicationstep.applicationstep_id = application_steps.applicationstep_id
    inner join application_last_steps
        on
            application_steps.course_id = application_last_steps.course_id
            and application_steps.applicationstep_step_order = application_last_steps.last_step
)

select
    applications.application_id
    , applications.user_id
    , applications.courserun_id
    , runs.courserun_readable_id
    , runs.courserun_title
    , runs.courserun_start_on
    , runs.courserun_end_on
    , applications.application_linkedin_url
    , applications.application_resume_file
    , applications.application_resume_uploaded_on
    , applications.application_state
    , applications.application_created_on
    , applications.application_updated_on
    , personal_prices.personalprice as personal_price
    , installments.list_price
    , fulfilled_orders.total_paid
    , case
        when applicationstep_submissions.submission_review_status = 'approved'
            then applicationstep_submissions.submission_reviewed_on
    end as application_admitted_date
from applications
inner join runs on applications.courserun_id = runs.courserun_id
left join courserun_last_steps
    on applications.courserun_id = courserun_last_steps.courserun_id
left join applicationstep_submissions
    on
        courserun_last_steps.courserun_applicationstep_id = applicationstep_submissions.courserun_applicationstep_id
        and applications.application_id = applicationstep_submissions.application_id
left join installments
    on applications.courserun_id = installments.courserun_id
left join personal_prices
    on
        applications.user_id = personal_prices.user_id
        and applications.courserun_id = personal_prices.courserun_id
left join fulfilled_orders
    on applications.application_id = fulfilled_orders.application_id
