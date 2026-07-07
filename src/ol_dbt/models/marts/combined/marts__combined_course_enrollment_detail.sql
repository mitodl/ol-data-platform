-- Migrated to use dimensional layer (tfact_enrollment, dim_course_run, dim_course, dim_user)
-- instead of intermediate combined models.
-- Grade data sourced from tfact_grade (joined on user_fk + courserun_fk).
-- Ecommerce order CTEs retained from int__ models (not yet in dimensional layer).
-- MITxPro order join uses a supplemental CTE for ecommerce_order_id (not in tfact_enrollment).
-- Bootcamps order join changed from username-based to user_id-based (username not in dim_user).
-- edX.org certificate MITxOnline cross-lookup uses dim_user.user_mitxonline_username + user_email.
-- Certificate join for all platforms uses the platform-specific username (restores
-- pre-migration int__combined__courserun_enrollments semantics); Bootcamps username is
-- joined in directly from int__bootcamps__users since dim_user doesn't expose it.

with enrollments as (
    select
        enrollment_key
        , enrollment_id
        , enrollment_type
        , user_fk
        , courserun_fk
        , platform
        , enrollment_is_active
        , enrollment_mode
        , enrollment_status
        , enrollment_created_on
        , enrollment_updated_on
        , enrollment_is_edx_enrolled
    from {{ ref('tfact_enrollment') }}
    where enrollment_type = 'course'
)

, course_runs as (
    select
        courserun_pk
        , source_id as courserun_source_id
        , course_fk
        , courserun_readable_id
        , courserun_title
        , courserun_start_on
        , courserun_end_on
        , courserun_upgrade_deadline
        , platform
        , {{ is_courserun_current('courserun_start_on', 'courserun_end_on') }} as courserun_is_current
    from {{ ref('dim_course_run') }}
    where is_current = true
)

, dim_course_cte as (
    select
        course_pk
        , course_readable_id
        , course_title
    from {{ ref('dim_course') }}
    where is_current = true
)

, users as (
    select
        user_pk
        , email as user_email
        , full_name as user_full_name
        , address_country as user_country_code
        , highest_education as user_highest_education
        , company as user_company
        , gender as user_gender
        , mitxonline_application_user_id
        , mitxpro_application_user_id
        , edxorg_openedx_user_id
        , bootcamps_application_user_id
        , emeritus_user_id
        , global_alumni_user_id
        , residential_openedx_user_id
        , micromasters_user_id
        , user_mitxonline_username
        , user_mitxpro_username
        , user_edxorg_username
    from {{ ref('dim_user') }}
)

, bootcamps_users as (
    -- dim_user does not expose a bootcamps username attribute; join directly to the
    -- intermediate model to restore user_username for Bootcamps enrollments/certificates.
    select user_id, user_username
    from {{ ref('int__bootcamps__users') }}
)

, grades as (
    select
        user_fk
        , courserun_fk
        , grade_value
        , is_passing
    from {{ ref('tfact_grade') }}
)

, combined_certificates as (
    select * from {{ ref('int__combined__courserun_certificates') }}
)

, mitxonline_certificates as (
    select * from {{ ref('int__mitxonline__courserun_certificates') }}
)

-- Keep supplemental CTE for MITxPro ecommerce_order_id (not stored in tfact_enrollment)
, mitxpro_enrollment_orders as (
    select
        courserunenrollment_id
        , ecommerce_order_id
    from {{ ref('int__mitxpro__courserunenrollments') }}
)

, mitxonline_transactions as (
    select
        order_id
        , max(transaction_timestamp) as payment_timestamp
    from {{ ref('int__mitxonline__ecommerce_transaction') }}
    where transaction_type = 'payment'
    group by order_id
)

, mitxonline_completed_orders as (
    select
        *
        , row_number() over (
            partition by user_id, courserun_id
            order by order_created_on desc
        ) as row_num
    from {{ ref('int__mitxonline__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, micromasters_completed_orders as (
    select
        *
        -- Partition matches the courserun_edxorg_readable_id join key below (not the
        -- MicroMasters-native courserun_readable_id, a different id namespace), otherwise
        -- row_num=1 can be assigned within the wrong partition and drop valid orders.
        , row_number() over (
            partition by user_id, courserun_edxorg_readable_id
            order by order_created_on desc, order_id desc
        ) as row_num
    from {{ ref('int__micromasters__orders') }}
    where order_state in ('fulfilled', 'refunded', 'partially_refunded')
)

, mitxpro_completed_orders as (
    select * from {{ ref('int__mitxpro__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, mitxpro_receipts as (
    select
        order_id
        , max(receipt_payment_timestamp) as payment_timestamp
    from {{ ref('int__mitxpro__ecommerce_receipt') }}
    where receipt_transaction_status != 'ERROR'
    group by order_id
)

, mitxpro__ecommerce_line as (
    select * from {{ ref('int__mitxpro__ecommerce_line') }}
)

, bootcamps_completed_orders as (
    select * from {{ ref('int__bootcamps__ecommerce_order') }}
    where order_state in ('fulfilled', 'refunded')
)

, bootcamps_receipts as (
    select
        order_id
        , max(receipt_payment_timestamp) as receipt_payment_timestamp
    from {{ ref('int__bootcamps__ecommerce_receipt') }}
    group by order_id
)

, enrollment_detail as (
    select
        enrollments.platform
        , enrollments.enrollment_id as courserunenrollment_id
        , enrollments.enrollment_is_active as courserunenrollment_is_active
        , enrollments.enrollment_created_on as courserunenrollment_created_on
        , enrollments.enrollment_mode as courserunenrollment_enrollment_mode
        , enrollments.enrollment_status as courserunenrollment_enrollment_status
        , enrollments.enrollment_is_edx_enrolled as courserunenrollment_is_edx_enrolled
        -- Platform-specific user_id (raw integer/varchar from the source platform)
        , case
            when enrollments.platform = '{{ var("mitxonline") }}'
                then cast(users.mitxonline_application_user_id as varchar)
            when enrollments.platform = '{{ var("edxorg") }}'
                then cast(users.edxorg_openedx_user_id as varchar)
            when enrollments.platform = '{{ var("mitxpro") }}'
                then cast(users.mitxpro_application_user_id as varchar)
            when enrollments.platform = '{{ var("bootcamps") }}'
                then cast(users.bootcamps_application_user_id as varchar)
            when enrollments.platform = '{{ var("emeritus") }}'
                then cast(users.emeritus_user_id as varchar)
            when enrollments.platform = '{{ var("global_alumni") }}'
                then cast(users.global_alumni_user_id as varchar)
            when enrollments.platform = '{{ var("residential") }}'
                then cast(users.residential_openedx_user_id as varchar)
        end as user_id
        -- Platform-specific username (used in final SELECT for user_username output)
        , case
            when enrollments.platform = '{{ var("mitxonline") }}'
                then users.user_mitxonline_username
            when enrollments.platform = '{{ var("edxorg") }}'
                then users.user_edxorg_username
            when enrollments.platform = '{{ var("mitxpro") }}'
                then users.user_mitxpro_username
            when enrollments.platform = '{{ var("residential") }}'
                then users.user_residential_username
            when enrollments.platform = '{{ var("bootcamps") }}'
                then bootcamps_users.user_username
            else null
        end as user_username
        , users.user_email
        , users.user_full_name
        , users.user_country_code
        , users.user_highest_education
        , users.user_company
        , users.user_gender
        -- Course run attributes from dim_course_run
        , course_runs.courserun_source_id as courserun_id
        , course_runs.courserun_readable_id
        , course_runs.courserun_title
        , course_runs.courserun_start_on
        , course_runs.courserun_end_on
        , course_runs.courserun_is_current
        , course_runs.courserun_upgrade_deadline
        -- Course attributes from dim_course (via dim_course_run.course_fk); fall back to
        -- extracting the course id from the course run id (matches int__combined__course_runs
        -- logic) rather than the run id itself when dim_course does not join.
        , coalesce(
            dim_course_cte.course_readable_id
            , {{ extract_course_readable_id('course_runs.courserun_readable_id') }}
        ) as course_readable_id
        , coalesce(dim_course_cte.course_title, course_runs.courserun_title) as course_title
        -- Grade from tfact_grade (user_fk + courserun_fk grain matches tfact_enrollment)
        , grades.grade_value as courserungrade_grade
        , grades.is_passing as courserungrade_is_passing
        -- Certificates: edX.org enrollments get MITxOnline certificate priority (MicroMasters linkage);
        -- all other platforms use combined_certificates directly.
        , case
            when enrollments.platform = '{{ var("edxorg") }}'
                then case
                    when mitxonline_certificates.courseruncertificate_is_revoked = false then true
                    when combined_certificates.courseruncertificate_created_on is not null then true
                    else false
                end
            else if(combined_certificates.courseruncertificate_url is not null, true, false)
        end as courseruncertificate_is_earned
        , coalesce(
            case when enrollments.platform = '{{ var("edxorg") }}'
                then mitxonline_certificates.courseruncertificate_created_on
            end
            , combined_certificates.courseruncertificate_created_on
        ) as courseruncertificate_created_on
        , coalesce(
            case when enrollments.platform = '{{ var("edxorg") }}'
                then mitxonline_certificates.courseruncertificate_issued_on
            end
            , combined_certificates.courseruncertificate_issued_on
        ) as courseruncertificate_issued_on
        , coalesce(
            case when enrollments.platform = '{{ var("edxorg") }}'
                then mitxonline_certificates.courseruncertificate_url
            end
            , combined_certificates.courseruncertificate_url
        ) as courseruncertificate_url
        , coalesce(
            case when enrollments.platform = '{{ var("edxorg") }}'
                then mitxonline_certificates.courseruncertificate_uuid
            end
            , combined_certificates.courseruncertificate_uuid
        ) as courseruncertificate_uuid
        -- Order data (per-platform LEFT JOINs below)
        , coalesce(
            case when enrollments.platform = '{{ var("mitxonline") }}'
                then mitxonline_completed_orders.order_id
            end
            , case when enrollments.platform = '{{ var("edxorg") }}'
                then micromasters_completed_orders.order_id
            end
            , case when enrollments.platform = '{{ var("mitxpro") }}'
                then mitxpro_completed_orders.order_id
            end
            , case when enrollments.platform = '{{ var("bootcamps") }}'
                then bootcamps_completed_orders.order_id
            end
        ) as order_id
        , coalesce(
            case when enrollments.platform = '{{ var("mitxonline") }}'
                then mitxonline_completed_orders.line_id
            end
            , case when enrollments.platform = '{{ var("edxorg") }}'
                then micromasters_completed_orders.line_id
            end
            , case when enrollments.platform = '{{ var("mitxpro") }}'
                then mitxpro__ecommerce_line.line_id
            end
            , case when enrollments.platform = '{{ var("bootcamps") }}'
                then bootcamps_completed_orders.line_id
            end
        ) as line_id
        , coalesce(
            case when enrollments.platform = '{{ var("mitxonline") }}'
                then mitxonline_completed_orders.order_reference_number
            end
            , case when enrollments.platform = '{{ var("edxorg") }}'
                then micromasters_completed_orders.order_reference_number
            end
            , case when enrollments.platform = '{{ var("mitxpro") }}'
                then mitxpro_completed_orders.receipt_reference_number
            end
            , case when enrollments.platform = '{{ var("bootcamps") }}'
                then bootcamps_completed_orders.order_reference_number
            end
        ) as order_reference_number
        , coalesce(
            case when enrollments.platform = '{{ var("mitxonline") }}'
                then mitxonline_completed_orders.discount_code
            end
            , case when enrollments.platform = '{{ var("edxorg") }}'
                then micromasters_completed_orders.coupon_code
            end
            , case when enrollments.platform = '{{ var("mitxpro") }}'
                then mitxpro_completed_orders.coupon_code
            end
        ) as coupon_code
        , case
            when enrollments.platform = '{{ var("mitxonline") }}'
                and mitxonline_completed_orders.order_id is not null
                then coalesce(mitxonline_transactions.payment_timestamp, mitxonline_completed_orders.order_created_on)
            when enrollments.platform = '{{ var("edxorg") }}'
                and micromasters_completed_orders.order_id is not null
                then coalesce(
                    micromasters_completed_orders.receipt_payment_timestamp
                    , micromasters_completed_orders.order_created_on
                )
            when enrollments.platform = '{{ var("mitxpro") }}'
                and mitxpro_completed_orders.order_id is not null
                then coalesce(mitxpro_receipts.payment_timestamp, mitxpro_completed_orders.order_created_on)
            when enrollments.platform = '{{ var("bootcamps") }}'
                and bootcamps_completed_orders.order_id is not null
                then coalesce(
                    bootcamps_receipts.receipt_payment_timestamp
                    , bootcamps_completed_orders.order_created_on
                )
        end as courserunenrollment_upgraded_on
    from enrollments
    left join course_runs on enrollments.courserun_fk = course_runs.courserun_pk
    left join dim_course_cte on course_runs.course_fk = dim_course_cte.course_pk
    left join users on enrollments.user_fk = users.user_pk
    left join bootcamps_users
        on enrollments.platform = '{{ var("bootcamps") }}'
        and users.bootcamps_application_user_id = bootcamps_users.user_id
    left join grades
        on enrollments.user_fk = grades.user_fk
        and enrollments.courserun_fk = grades.courserun_fk
    -- Certificate join: match on the platform-specific username (restores pre-migration
    -- int__combined__courserun_enrollments semantics); email is a deduped/canonicalized
    -- value on dim_user and can silently miss platform-specific certificate records.
    left join combined_certificates
        on combined_certificates.platform = enrollments.platform
        and combined_certificates.courserun_readable_id = course_runs.courserun_readable_id
        and combined_certificates.user_username = case
            when enrollments.platform = '{{ var("mitxonline") }}'
                then users.user_mitxonline_username
            when enrollments.platform = '{{ var("edxorg") }}'
                then users.user_edxorg_username
            when enrollments.platform = '{{ var("mitxpro") }}'
                then users.user_mitxpro_username
            when enrollments.platform = '{{ var("bootcamps") }}'
                then bootcamps_users.user_username
        end
    -- edX.org: also check MITxOnline certificate table for MicroMasters-linked certs
    left join mitxonline_certificates
        on enrollments.platform = '{{ var("edxorg") }}'
        and (
            users.user_mitxonline_username = mitxonline_certificates.user_username
            or users.user_email = mitxonline_certificates.user_email
        )
        and course_runs.courserun_readable_id
            = replace(replace(mitxonline_certificates.courserun_readable_id, 'course-v1:', ''), '+', '/')
    -- MITxOnline orders: join on platform user_id + courserun source_id
    left join mitxonline_completed_orders
        on enrollments.platform = '{{ var("mitxonline") }}'
        and users.mitxonline_application_user_id = mitxonline_completed_orders.user_id
        and course_runs.courserun_source_id = mitxonline_completed_orders.courserun_id
        and mitxonline_completed_orders.row_num = 1
    left join mitxonline_transactions
        on mitxonline_completed_orders.order_id = mitxonline_transactions.order_id
    -- edX.org (MicroMasters) orders: join on micromasters_user_id stored in dim_user
    left join micromasters_completed_orders
        on enrollments.platform = '{{ var("edxorg") }}'
        and users.micromasters_user_id = micromasters_completed_orders.user_id
        and course_runs.courserun_readable_id = micromasters_completed_orders.courserun_edxorg_readable_id
        and micromasters_completed_orders.row_num = 1
    -- MITxPro orders: bridged via supplemental ecommerce_order_id CTE
    left join mitxpro_enrollment_orders
        on enrollments.platform = '{{ var("mitxpro") }}'
        and enrollments.enrollment_id = cast(mitxpro_enrollment_orders.courserunenrollment_id as varchar)
    left join mitxpro_completed_orders
        on mitxpro_enrollment_orders.ecommerce_order_id = mitxpro_completed_orders.order_id
    left join mitxpro__ecommerce_line
        on mitxpro_completed_orders.order_id = mitxpro__ecommerce_line.order_id
    left join mitxpro_receipts
        on mitxpro_completed_orders.order_id = mitxpro_receipts.order_id
    -- Bootcamps orders: join on bootcamps_application_user_id (username not in dim_user)
    left join bootcamps_completed_orders
        on enrollments.platform = '{{ var("bootcamps") }}'
        and users.bootcamps_application_user_id = bootcamps_completed_orders.order_purchaser_user_id
        and course_runs.courserun_source_id = bootcamps_completed_orders.courserun_id
    left join bootcamps_receipts
        on bootcamps_completed_orders.order_id = bootcamps_receipts.order_id
)

select
    platform
    , courserunenrollment_id
    , {{ generate_hash_id('cast(order_id as varchar)
        || cast(coalesce(line_id, 9) as varchar)
        || platform') }} as combined_orders_hash_id
    , course_readable_id
    , course_title
    , courserun_id
    , courserun_is_current
    , courserun_readable_id
    , courserun_start_on
    , courserun_end_on
    , courserun_title
    , courserun_upgrade_deadline
    , courseruncertificate_created_on
    , courseruncertificate_issued_on
    , courseruncertificate_is_earned
    , courseruncertificate_url
    , courseruncertificate_uuid
    , courserunenrollment_created_on
    , courserunenrollment_enrollment_mode
    , courserunenrollment_enrollment_status
    , courserunenrollment_is_active
    , courserunenrollment_is_edx_enrolled
    , courserunenrollment_upgraded_on
    , courserungrade_grade
    , courserungrade_is_passing
    , line_id
    , order_id
    , order_reference_number
    , coupon_code
    , user_company
    , user_country_code
    , user_email
    , user_full_name
    , user_highest_education
    , user_gender
    -- identity ordering must match int__combined__users: global_alumni's user_id
    --  (their student_id) is not reliably unique per person, so user_email is
    --  preferred there; every other platform uses the default user_id-first order.
    , {{ generate_hash_id('
        case
            when platform = \'' ~ var("global_alumni") ~ '\'
                then coalesce(user_email, user_id, user_full_name)
            else coalesce(user_id, user_email, user_full_name)
        end || platform') }} as user_hashed_id
    , user_id
    , user_username
from enrollment_detail
