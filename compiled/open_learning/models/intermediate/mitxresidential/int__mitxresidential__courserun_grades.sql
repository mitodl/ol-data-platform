with grades as (
    select * from dev.main_staging.stg__mitxresidential__openedx__courserun_grade
)

, runs as (
    select * from dev.main_staging.stg__mitxresidential__openedx__courserun
)

, users as (
    select * from dev.main_staging.stg__mitxresidential__openedx__auth_user
)

, courserun_grades as (
    select
        grades.courserungrade_id
        , grades.courserun_readable_id
        , runs.courserun_title
        , grades.courserungrade_grade
        , grades.courserungrade_letter_grade
        , grades.courserungrade_created_on
        , grades.courserungrade_updated_on
        , grades.user_id
        , users.user_username
        , users.user_email
    from grades
    left join runs on grades.courserun_readable_id = runs.courserun_readable_id
    inner join users on grades.user_id = users.user_id
)

select * from courserun_grades
