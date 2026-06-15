with enrollment as (
    select * from {{ ref('tfact_enrollment') }}
)

, course_run as (
    select * from {{ ref('dim_course_run') }}
)

, course as (
    select * from {{ ref('dim_course') }}
)

, f_certificate as (
    select * from {{ ref('tfact_certificate') }}
    where certificate_scope = 'course'
)

, grade as (
    select * from {{ ref('tfact_grade') }}
)

, organization_courserun as (
    select * from {{ ref('bridge_organization_courserun') }}
)

, organization as (
    select * from {{ ref('dim_organization') }}
)

, d_user as (
    select * from {{ ref('dim_user') }}
)

, product as (
    select * from {{ ref('dim_product') }}
)

, f_order as (
    select * from {{ ref('tfact_order') }}
)

, discount as (
    select * from {{ ref('dim_discount') }}
)

, payment as (
    select * from {{ ref('tfact_payment') }}
)

, payment_method as (
    select * from {{ ref('dim_payment_method') }}
)

, d_user_payer as (
    select * from {{ ref('dim_user') }}
)

, program as (
    select * from {{ ref('dim_program') }}
)

, discount_type as (
    select * from {{ ref('dim_discount_type') }}
)

, combined_discounts as (
    select * from {{ ref('marts__combined_discounts') }}
)

, discount_names as (
    select
        discount_code
        , discount_name
    from combined_discounts
    group by
        discount_code
        , discount_name
)

, combined__orders as (
    select * from {{ ref('marts__combined__orders') }}
)

, order_emails as (
    select
        order_id
        , platform
        , redeemed_email
    from combined__orders
    group by
        order_id
        , platform
        , redeemed_email
)

, combined_enrollment_detail as (
    select * from {{ ref('marts__combined_course_enrollment_detail') }}
)

, enrollment_upgrades as (
    select
        order_id
        , platform
        , courserun_readable_id
        , user_email
        , max(courserunenrollment_upgraded_on) as courserunenrollment_upgraded_on
    from combined_enrollment_detail
    group by
        order_id
        , platform
        , courserun_readable_id
        , user_email
)

select
    enrollment.platform
    , enrollment_id as courserunenrollment_id
    , course_readable_id
    , course_title
    , course_run.is_current as courserun_is_current
    , courserun_readable_id
    , courserun_start_on
    , courserun_end_on
    , courserun_title
    , enrollment_created_on as courserunenrollment_created_on
    , enrollment_mode as courserunenrollment_enrollment_mode
    , enrollment_status as courserunenrollment_enrollment_status
    , enrollment_is_active as courserunenrollment_is_active
    , enrollment_upgrades.courserunenrollment_upgraded_on
    , certificate_created_on as courseruncertificate_created_on
    , certificate_issued_on as courseruncertificate_issued_on
    , case when certificate_issued_on is not null then true else false end as courseruncertificate_is_earned
    , case when certificate_uuid is not null
        then concat('https://mitxonline.mit.edu/certificate/', certificate_uuid)
        else null end as courseruncertificate_url
    , grade_value as courserungrade_grade
    , is_passing as courserungrade_is_passing
    , organization_key
    , organization_name
    , d_user.address_country as user_country_code
    , d_user.highest_education as user_highest_education
    , d_user.full_name as user_full_name
    , case enrollment.platform
        when 'mitxonline' then d_user.user_mitxonline_username
        when 'mitxpro' then d_user.user_mitxpro_username
        when 'residential' then d_user.user_residential_username
        when 'edxorg' then d_user.user_edxorg_username
        else null
      end as user_username
    , d_user.email as user_email
    , discount.discount_code as coupon_code
    , discount_names.discount_name as coupon_name
    , discount_amount as discount
    , f_order.order_id
    , f_order.order_reference_number
    , order_state
    , f_order.order_updated_on
    , payment.transaction_amount as receipt_payment_amount
    , payment_method_name as receipt_payment_method
    , transaction_created_on as receipt_payment_timestamp
    , d_user_payer.email as receipt_payer_email
    , line_price as unit_price
    , program_name
    , discount_type_name
    , order_emails.redeemed_email
    , if(
        enrollment.platform = '{{ var("mitxonline") }}'
        , concat('https://mitxonline.mit.edu/orders/receipt/', cast(f_order.order_id as varchar))
        , null
    ) as receipt_url
from enrollment
inner join course_run
    on enrollment.courserun_fk = course_run.courserun_pk
inner join course
    on course_run.course_fk=course.course_pk
left join f_certificate
    on
        enrollment.user_fk = f_certificate.user_fk
        and enrollment.courserun_fk = f_certificate.courserun_fk
        and enrollment.platform_fk = f_certificate.platform_fk
left join grade
    on
        enrollment.user_fk = grade.user_fk
        and enrollment.courserun_fk = grade.courserun_fk
        and enrollment.platform_fk = grade.platform_fk
left join organization_courserun
    on enrollment.courserun_fk = organization_courserun.courserun_fk
left join organization
    on organization_courserun.organization_fk = organization.organization_pk
inner join d_user
    on enrollment.user_fk = d_user.user_pk
left join product
    on
        (enrollment.courserun_fk = product.courserun_fk
        or enrollment.program_fk = product.program_fk)
        and enrollment.platform_fk = product.platform_fk
left join f_order
    on
        enrollment.user_fk = f_order.user_fk
        and product.product_pk = f_order.product_fk
        and enrollment.platform_fk = f_order.platform_fk
left join discount
    on f_order.discount_fk = discount.discount_pk
left join payment
    on
        f_order.order_id = payment.order_id
        and f_order.platform_fk = payment.platform_fk
left join payment_method
    on payment.payment_method_fk = payment_method.payment_method_pk
left join d_user_payer
    on payment.user_fk = d_user_payer.user_pk
left join program
    on enrollment.program_fk = program.program_pk
left join discount_type
    on f_order.discount_type_fk = discount_type.discount_type_pk
left join discount_names
    on discount.discount_code = discount_names.discount_code
left join enrollment_upgrades
    on f_order.order_id = enrollment_upgrades.order_id
    and enrollment.platform = enrollment_upgrades.platform
    and course_run.courserun_readable_id = enrollment_upgrades.courserun_readable_id
    and d_user.email = enrollment_upgrades.user_email
left join order_emails
    on f_order.order_id = order_emails.order_id
    and case enrollment.platform
        when 'bootcamps' then '{{ var("bootcamps") }}'
        when 'edxorg' then '{{ var("edxorg") }}'
        when 'emeritus' then '{{ var("emeritus") }}'
        when 'global_alumni' then '{{ var("global_alumni") }}'
        when 'mitxonline' then '{{ var("mitxonline") }}'
        when 'mitxpro' then '{{ var("mitxpro") }}'
        when 'residential' then '{{ var("residential") }}'
        else enrollment.platform
    end = order_emails.platform
    and course_run.courserun_readable_id = order_emails.courserun_readable_id
    and d_user.email = order_emails.user_email
