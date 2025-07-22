with subsection_grades as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__grades_subsectiongrade') }}
)

, subsection_grade_overrides as (
    select * from {{ ref('stg__mitxonline__openedx__mysql__grades_subsectiongradeoverride') }}
)

, course_structure as (
    select
        *
        , row_number() over (
            partition by courserun_readable_id, coursestructure_block_id
            order by coursestructure_retrieved_at desc
        ) as row_num
    from {{ ref('int__mitxonline__course_structure') }}
)

, subsection_grades_joined as (
    select
        subsection_grades.courserun_readable_id
        , subsection_grades.coursestructure_block_id
        , subsection_grades.visibleblocks_hash
        , course_structure.coursestructure_block_title
        , course_structure.coursestructure_chapter_id
        , subsection_grades.openedx_user_id
        , subsection_grades.subsectiongrade_first_attempted_on
        , subsection_grades.subsectiongrade_created_on
        , coalesce(
            subsection_grade_overrides.subsectiongradeoverride_total_score
            , subsection_grades.subsectiongrade_total_score
        ) as subsectiongrade_total_score
        , coalesce(
            subsection_grade_overrides.subsectiongradeoverride_total_earned_score
            , subsection_grades.subsectiongrade_total_earned_score
        ) as subsectiongrade_total_earned_score
        , coalesce(
            subsection_grade_overrides.subsectiongradeoverride_total_graded_score
            , subsection_grades.subsectiongrade_total_graded_score
        ) as subsectiongrade_total_graded_score
        , coalesce(
            subsection_grade_overrides.subsectiongradeoverride_total_earned_graded_score
            , subsection_grades.subsectiongrade_total_earned_graded_score
        ) as subsectiongrade_total_earned_graded_score
        , if(subsection_grade_overrides.subsectiongradeoverride_id is not null, true, false)
            as subsectiongrade_is_overridden
    from subsection_grades
    inner join course_structure
        on subsection_grades.coursestructure_block_id = course_structure.coursestructure_block_id
    left join subsection_grade_overrides
        on subsection_grades.subsectiongrade_id = subsection_grade_overrides.subsectiongrade_id
    where course_structure.row_num = 1
)

select * from subsection_grades_joined
