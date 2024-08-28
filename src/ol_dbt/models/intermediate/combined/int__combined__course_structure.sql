with mitxonline_course_structure as (
    select * from {{ ref('int__mitxonline__course_structure') }}
)

, edxorg_course_structure as (
    select * from {{ ref('int__edxorg__mitx_course_structure') }}
)

, xpro_course_structure as (
    select * from {{ ref('int__mitxpro__course_structure') }}
)


, combined as (
    select
        '{{ var("mitxonline") }}' as platform
        , courserun_readable_id
        , courserun_title
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
        , coursestructure_chapter_id
        , coursestructure_chapter_title
    from mitxonline_course_structure
    where coursestructure_is_latest = true

    union all

    select
        '{{ var("edxorg") }}' as platform
        , courserun_readable_id
        , courserun_title
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
        , coursestructure_chapter_id
        , coursestructure_chapter_title
    from edxorg_course_structure
    where coursestructure_is_latest = true

    union all

    select
        '{{ var("mitxpro") }}' as platform
        , courserun_readable_id
        , courserun_title
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
        , coursestructure_chapter_id
        , coursestructure_chapter_title
    from xpro_course_structure
    where coursestructure_is_latest = true
)

select * from combined
