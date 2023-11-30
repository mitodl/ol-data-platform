with course_structure as (
    select * from {{ ref('stg__mitxonline__openedx__api__course_structure') }}
    order by courserun_readable_id, coursestructure_retrieved_at, coursestructure_block_index

)

, chapters as (
    select * from course_structure
    where coursestructure_block_category = 'chapter'
)

, course_structure_with_chapters as (
    select
        course_structure.*
        , chapters.coursestructure_block_id as coursestructure_chapter_id
        , row_number() over (
            partition by
                course_structure.courserun_readable_id
                , course_structure.coursestructure_block_index
                , course_structure.coursestructure_retrieved_at
            order by chapters.coursestructure_block_index desc
        ) as row_num
    from course_structure
    inner join chapters
        on
            course_structure.courserun_readable_id = chapters.courserun_readable_id
            and course_structure.coursestructure_retrieved_at = chapters.coursestructure_retrieved_at
            and course_structure.coursestructure_block_index >= chapters.coursestructure_block_index
)

select
    course_structure.courserun_readable_id
    , course_structure.courserun_title
    , course_structure.coursestructure_block_index
    , course_structure.coursestructure_block_id
    , course_structure.coursestructure_parent_block_id
    , course_structure.coursestructure_block_category
    , course_structure.coursestructure_block_title
    , course_structure.coursestructure_content_hash
    , course_structure.coursestructure_block_content_hash
    , course_structure.coursestructure_block_metadata
    , course_structure.courserun_start_on
    , course_structure.coursestructure_retrieved_at
    , course_structure_with_chapters.coursestructure_chapter_id
from course_structure
left join course_structure_with_chapters
    on
        course_structure.courserun_readable_id = course_structure_with_chapters.courserun_readable_id
        and course_structure.coursestructure_block_id = course_structure_with_chapters.coursestructure_block_id
        and course_structure.coursestructure_retrieved_at = course_structure_with_chapters.coursestructure_retrieved_at
        and course_structure_with_chapters.row_num = 1
