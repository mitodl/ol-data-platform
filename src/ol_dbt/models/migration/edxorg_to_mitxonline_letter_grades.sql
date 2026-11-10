with edxorg_grade as (
    select
        cast(user_id as varchar) as user_edxorg_id
        -- The export mixes {org}/{course}/{run} and course-v1 IDs
        , case
            when courserun_readable_id like 'course-v1:%' then courserun_readable_id
            else {{ format_course_id('courserun_readable_id', false) }}
        end as courserun_readable_id
        -- Some course grading policies have a zero-width space before the label
        , max(trim(replace(courserungrade_letter_grade, chr(8203), ''))) as courserungrade_letter_grade
    from {{ ref('stg__edxorg__s3__courserun_grade') }}
    where trim(replace(courserungrade_letter_grade, chr(8203), '')) <> ''
    group by 1, 2
)

, mitx_users as (
    select * from {{ ref('int__mitx__users') }}
)

-- Same two user links as edxorg_to_mitxonline_enrollments: the linked account, and an
-- MITx Online account with the edX.org email
, user_links as (
    select
        user_mitxonline_id
        , cast(user_edxorg_id as varchar) as user_edxorg_id
    from mitx_users
    where user_mitxonline_id is not null and user_edxorg_id is not null

    union

    select
        mitxonline_user.user_mitxonline_id
        , cast(edxorg_user.user_edxorg_id as varchar) as user_edxorg_id
    from mitx_users as edxorg_user
    inner join mitx_users as mitxonline_user
        on lower(edxorg_user.user_edxorg_email) = lower(mitxonline_user.user_mitxonline_email)
    where mitxonline_user.user_mitxonline_id is not null and edxorg_user.user_edxorg_id is not null
)

, mitxonline_grade as (
    select
        grade.user_id
        , grade.courserun_id
        , course_runs.courserun_readable_id
    from {{ ref('stg__mitxonline__app__postgres__courses_courserungrade') }} as grade
    inner join {{ ref('int__mitxonline__course_runs') }} as course_runs
        on grade.courserun_id = course_runs.courserun_id
    where
        coalesce(grade.courserungrade_letter_grade, '') = ''
        and grade.courserungrade_is_set_by_admin = false
)

select
    mitxonline_grade.user_id as user_mitxonline_id
    , mitxonline_grade.courserun_id
    , mitxonline_grade.courserun_readable_id
    , max(edxorg_grade.courserungrade_letter_grade) as courserungrade_letter_grade
from mitxonline_grade
inner join user_links
    on mitxonline_grade.user_id = user_links.user_mitxonline_id
inner join edxorg_grade
    on user_links.user_edxorg_id = edxorg_grade.user_edxorg_id
    and mitxonline_grade.courserun_readable_id = edxorg_grade.courserun_readable_id
group by 1, 2, 3
