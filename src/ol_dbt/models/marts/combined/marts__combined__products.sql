with mitxonline_product as (
    select * from {{ ref('int__mitxonline__ecommerce_product') }}
)

, mitxonline_course_runs as (
    select * from {{ ref('int__mitxonline__course_runs') }}
)

, mitxonline_courses as (
    select * from {{ ref('int__mitxonline__courses') }}
)

, mitxonline_programs as (
    select * from {{ ref('int__mitxonline__programs') }}
)

, mitxpro_product as (
    select * from {{ ref('int__mitxpro__ecommerce_product') }}
)

, mitxpro_course_runs as (
    select * from {{ ref('int__mitxpro__course_runs') }}
)

, mitxpro_courses as (
    select * from {{ ref('int__mitxpro__courses') }}
)

, mitxpro_programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

, mitxpro_program_runs as (
    select * from {{ ref('int__mitxpro__program_runs') }}
)

, mitxonline_product_view as (
    select
        mitxonline_product.product_id
        , coalesce(mitxonline_product.product_type, 'course run') as product_type
        , coalesce(
            mitxonline_product.courserun_readable_id, mitxonline_course_runs.courserun_readable_id
        ) as product_readable_id
        , coalesce(cast(mitxonline_product.product_price as varchar), mitxonline_courses.course_price) as list_price
        , mitxonline_product.product_description
        , mitxonline_product.product_is_active
        , mitxonline_product.product_created_on
        , mitxonline_course_runs.courserun_title as product_name
        , mitxonline_course_runs.courserun_start_on as start_on
        , mitxonline_course_runs.courserun_end_on as end_on
        , mitxonline_course_runs.courserun_enrollment_start_on as enrollment_start_on
        , mitxonline_course_runs.courserun_enrollment_end_on as enrollment_end_on
        , mitxonline_course_runs.courserun_upgrade_deadline as upgrade_deadline
        , mitxonline_courses.course_length as duration
        , mitxonline_courses.course_effort as time_commitment
        , mitxonline_courses.course_certification_type as certification_type
        , mitxonline_courses.course_topics as topics
        , mitxonline_courses.course_instructors as instructors
        , if(mitxonline_course_runs.courserun_is_self_paced = true, 'Self-paced', 'Instructor-paced')
        as pace
        , if(
            mitxonline_courses.course_page_is_live = true and mitxonline_courses.course_is_live = true
            , true
            , false
        ) as is_live
    from mitxonline_course_runs
    inner join mitxonline_courses
        on mitxonline_course_runs.course_id = mitxonline_courses.course_id
    left join mitxonline_product
        on mitxonline_course_runs.courserun_id = mitxonline_product.courserun_id


    union all

    select
        null as product_id
        , 'program' as product_type
        , mitxonline_programs.program_readable_id as product_readable_id
        , null as list_price
        , mitxonline_programs.program_description as product_description
        , mitxonline_product.product_is_active
        , null as product_created_on
        , mitxonline_programs.program_title as product_name
        , null as start_on
        , null as end_on
        , null as enrollment_start_on
        , null as enrollment_end_on
        , null as upgrade_deadline
        , mitxonline_programs.program_length as duration
        , mitxonline_programs.program_effort as time_commitment
        , mitxonline_programs.program_certification_type as certification_type
        , mitxonline_programs.program_topics as topics
        , mitxonline_programs.program_instructors as instructors
        , null as pace
        , if(
            mitxonline_programs.program_is_live = true and mitxonline_programs.program_page_is_live = true
            , true
            , false
        ) as is_live
    from mitxonline_programs
    left join mitxonline_product
        on mitxonline_programs.program_readable_id = mitxonline_product.program_readable_id
    where mitxonline_product.program_readable_id is null
)

, mitxpro_product_view as (
    select
        mitxpro_product.product_id
        , cast(mitxpro_product.product_list_price as varchar) as list_price
        , mitxpro_product.product_description
        , mitxpro_product.product_is_private
        , mitxpro_product.product_is_active
        , mitxpro_product.product_created_on
        , mitxpro_course_runs.courserun_enrollment_start_on as enrollment_start_on
        , mitxpro_course_runs.courserun_enrollment_end_on as enrollment_end_on
        , mitxpro_courses.course_topics as topics
        , coalesce(mitxpro_courses.cms_coursepage_duration, mitxpro_programs.cms_programpage_duration) as duration
        , coalesce(
            mitxpro_courses.cms_coursepage_time_commitment, mitxpro_programs.cms_programpage_time_commitment
        ) as time_commitment
        , coalesce(mitxpro_courses.cms_coursepage_format, mitxpro_programs.cms_programpage_format) as delivery
        , coalesce(mitxpro_courses.cms_certificate_ceus, mitxpro_programs.cms_certificate_ceus)
        as continuing_education_credits
        , coalesce(mitxpro_courses.course_instructors, mitxpro_programs.program_instructors) as instructors
        , if(mitxpro_product.product_type = 'program', 'program run', mitxpro_product.product_type) as product_type
        , coalesce(mitxpro_courses.platform_name, mitxpro_programs.platform_name) as product_platform
        , coalesce(
            mitxpro_course_runs.courserun_readable_id, mitxpro_program_runs.programrun_readable_id
        ) as product_readable_id
        , coalesce(mitxpro_course_runs.courserun_title, mitxpro_program_runs.program_title) as product_name
        , coalesce(mitxpro_course_runs.courserun_start_on, mitxpro_program_runs.programrun_start_on) as start_on
        , coalesce(mitxpro_course_runs.courserun_end_on, mitxpro_program_runs.programrun_end_on) as end_on
        , case
            when mitxpro_courses.course_is_live = true and mitxpro_courses.cms_coursepage_is_live = true
                then true
            when mitxpro_programs.program_is_live = true and mitxpro_programs.cms_programpage_is_live = true
                then true
            else false
        end as is_live
    from mitxpro_product
    left join mitxpro_course_runs
        on mitxpro_product.courserun_id = mitxpro_course_runs.courserun_id
    left join mitxpro_courses
        on mitxpro_course_runs.course_id = mitxpro_courses.course_id
    left join mitxpro_program_runs
        on mitxpro_product.program_id = mitxpro_program_runs.program_id
    left join mitxpro_programs
        on mitxpro_program_runs.program_id = mitxpro_programs.program_id
    where mitxpro_product.product_type in ('program', 'course run')
)

select
    '{{ var("mitxonline") }}' as platform
    , '{{ var("mitxonline") }}' as product_platform
    , product_readable_id
    , product_name
    , product_id
    , product_type
    , product_description
    , list_price
    , product_is_active
    , false as product_is_private
    , product_created_on
    , start_on
    , end_on
    , enrollment_start_on
    , enrollment_end_on
    , upgrade_deadline
    , pace
    , duration
    , time_commitment
    , certification_type
    , 'Online' as delivery
    , null as continuing_education_credits
    , topics
    , instructors
    , 'MITx' as offered_by
    , is_live
from mitxonline_product_view

union all

select
    '{{ var("mitxpro") }}' as platform
    , product_platform
    , product_readable_id
    , product_name
    , product_id
    , product_type
    , product_description
    , list_price
    , product_is_active
    , product_is_private
    , product_created_on
    , start_on
    , end_on
    , enrollment_start_on
    , enrollment_end_on
    , null as upgrade_deadline
    , null as pace
    , duration
    , time_commitment
    , 'Professional Certificate' as certification_type
    , delivery
    , continuing_education_credits
    , topics
    , instructors
    , 'xPro' as offered_by
    , is_live
from mitxpro_product_view
