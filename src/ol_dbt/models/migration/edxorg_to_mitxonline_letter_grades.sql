-- Same user matching as edxorg_to_mitxonline_enrollments: prefer the MITx Online account
-- with the edX.org email, then the linked account
with edxorg_grade as (
    select
        coalesce(mitx_users_by_email.user_mitxonline_id, mitx__users.user_mitxonline_id) as user_mitxonline_id
        -- The export mixes {org}/{course}/{run} and course-v1 IDs
        , {{ format_course_id("replace(edxorg_grade.courserun_readable_id, 'course-v1:', '')", false) }}
            as courserun_readable_id
        -- Some course grading policies have a zero-width space before the label
        , replace(edxorg_grade.courserungrade_letter_grade, chr(8203), '') as courserungrade_letter_grade
    from {{ ref('stg__edxorg__s3__courserun_grade') }} as edxorg_grade
    inner join {{ ref('int__mitx__users') }} as mitx__users
        on cast(edxorg_grade.user_id as varchar) = cast(mitx__users.user_edxorg_id as varchar)
    left join {{ ref('int__mitx__users') }} as mitx_users_by_email
        on lower(mitx__users.user_edxorg_email) = lower(mitx_users_by_email.user_mitxonline_email)
)

select
    grade.user_id as user_mitxonline_id
    , grade.courserun_id
    , course_runs.courserun_readable_id
    , edxorg_grade.courserungrade_letter_grade
from {{ ref('stg__mitxonline__app__postgres__courses_courserungrade') }} as grade
inner join {{ ref('int__mitxonline__course_runs') }} as course_runs
    on grade.courserun_id = course_runs.courserun_id
inner join edxorg_grade
    on grade.user_id = edxorg_grade.user_mitxonline_id
    and course_runs.courserun_readable_id = edxorg_grade.courserun_readable_id
where
    course_runs.courserun_readable_id like 'course-v1:MITx+%'
    and coalesce(grade.courserungrade_letter_grade, '') = ''
    and edxorg_grade.courserungrade_letter_grade <> ''
