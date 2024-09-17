with mitx_courserun_enroll_w_programs as (
    select * from {{ ref('int__mitx__courserun_enrollments_with_programs') }}
)

, mitx__courses as (
    select * from {{ ref('int__mitx__courses') }}
)

, mitxpro__courses as (
    select * from {{ ref('int__mitxpro__courses') }}
)

, mitxpro__programs as (
    select * from {{ ref('int__mitxpro__programs') }}
)

select
    mitx_courserun_enroll_w_programs.platform
    , mitx__courses.course_title
    , mitx_courserun_enroll_w_programs.program_title
    , case
        when mitx_courserun_enroll_w_programs.platform = 'MITx Online'
            then mitx__courses.mitxonline_course_id
    end as course_id
    , case
        when mitx_courserun_enroll_w_programs.platform = 'MITx Online'
            then mitx_courserun_enroll_w_programs.mitxonline_program_id
    end as program_id
    , mitx__courses.course_readable_id
from mitx_courserun_enroll_w_programs
inner join mitx__courses
    on mitx_courserun_enroll_w_programs.course_number = mitx__courses.course_number

union all

select
    'xPRO' as platform
    , mitxpro__courses.course_title
    , mitxpro__programs.program_title
    , mitxpro__courses.course_id
    , mitxpro__courses.program_id
    , mitxpro__courses.course_readable_id
from mitxpro__courses
left join mitxpro__programs
    on mitxpro__courses.program_id = mitxpro__programs.program_id
