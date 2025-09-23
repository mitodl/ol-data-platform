with
    mitxonline_course_structure as (select * from {{ ref("stg__mitxonline__openedx__api__course_structure") }}),
    edxorg_course_structure as (select * from {{ ref("stg__edxorg__s3__course_structure") }}),
    xpro_course_structure as (select * from {{ ref("stg__mitxpro__openedx__api__course_structure") }}),
    residential_course_structure as (select * from {{ ref("stg__mitxresidential__openedx__api__course_structure") }}),
    combined as (
        select
            courserun_readable_id,
            coursestructure_block_index,
            coursestructure_block_id,
            coursestructure_parent_block_id,
            coursestructure_block_category,
            coursestructure_block_title,
            coursestructure_block_metadata,
            coursestructure_retrieved_at
        from mitxonline_course_structure

        union distinct

        select
            courserun_readable_id,
            coursestructure_block_index,
            coursestructure_block_id,
            coursestructure_parent_block_id,
            coursestructure_block_category,
            coursestructure_block_title,
            coursestructure_block_metadata,
            coursestructure_retrieved_at
        from edxorg_course_structure

        union distinct

        select
            courserun_readable_id,
            coursestructure_block_index,
            coursestructure_block_id,
            coursestructure_parent_block_id,
            coursestructure_block_category,
            coursestructure_block_title,
            coursestructure_block_metadata,
            coursestructure_retrieved_at
        from xpro_course_structure

        union distinct

        select
            courserun_readable_id,
            coursestructure_block_index,
            coursestructure_block_id,
            coursestructure_parent_block_id,
            coursestructure_block_category,
            coursestructure_block_title,
            coursestructure_block_metadata,
            coursestructure_retrieved_at
        from residential_course_structure
    ),
    latest_course_structure as (
        select courserun_readable_id, max(coursestructure_retrieved_at) as max_retrieved_date
        from combined
        group by courserun_readable_id
    ),
    sequentials as (select * from combined where coursestructure_block_category = 'sequential'),
    combined_with_sequentials as (
        select
            combined.*,
            sequentials.coursestructure_block_id as sequential_block_id,
            sequentials.coursestructure_block_title as sequential_title,
            row_number() over (
                partition by
                    combined.courserun_readable_id,
                    combined.coursestructure_block_index,
                    combined.coursestructure_retrieved_at
                order by sequentials.coursestructure_block_index desc
            ) as row_num
        from combined
        inner join
            sequentials
            on combined.courserun_readable_id = sequentials.courserun_readable_id
            and combined.coursestructure_retrieved_at = sequentials.coursestructure_retrieved_at
            and combined.coursestructure_block_index >= sequentials.coursestructure_block_index
    ),
    chapters as (select * from combined where coursestructure_block_category = 'chapter'),
    combined_with_chapters as (
        select
            combined.*,
            chapters.coursestructure_block_id as chapter_block_id,
            chapters.coursestructure_block_title as chapter_title,
            row_number() over (
                partition by
                    combined.courserun_readable_id,
                    combined.coursestructure_block_index,
                    combined.coursestructure_retrieved_at
                order by chapters.coursestructure_block_index desc
            ) as row_num
        from combined
        inner join
            chapters
            on combined.courserun_readable_id = chapters.courserun_readable_id
            and combined.coursestructure_retrieved_at = chapters.coursestructure_retrieved_at
            and combined.coursestructure_block_index >= chapters.coursestructure_block_index
    ),
    combined_course_content as (
        select
            combined.courserun_readable_id,
            combined.coursestructure_block_index as block_index,
            combined.coursestructure_block_id as block_id,
            combined.coursestructure_parent_block_id as parent_block_id,
            combined.coursestructure_block_category as block_category,
            combined.coursestructure_block_title as block_title,
            combined.coursestructure_block_metadata as block_metadata,
            combined.coursestructure_retrieved_at as retrieved_at,
            combined_with_chapters.chapter_block_id,
            combined_with_chapters.chapter_title,
            combined_with_sequentials.sequential_block_id,
            combined_with_sequentials.sequential_title,
            if(latest_course_structure.max_retrieved_date is not null, true, false) as is_latest
        from combined
        left join
            latest_course_structure
            on combined.courserun_readable_id = latest_course_structure.courserun_readable_id
            and combined.coursestructure_retrieved_at = latest_course_structure.max_retrieved_date
        left join
            combined_with_chapters
            on combined.courserun_readable_id = combined_with_chapters.courserun_readable_id
            and combined.coursestructure_block_id = combined_with_chapters.coursestructure_block_id
            and combined.coursestructure_retrieved_at = combined_with_chapters.coursestructure_retrieved_at
            and combined_with_chapters.row_num = 1
        left join
            combined_with_sequentials
            on combined.courserun_readable_id = combined_with_sequentials.courserun_readable_id
            and combined.coursestructure_block_id = combined_with_sequentials.coursestructure_block_id
            and combined.coursestructure_retrieved_at = combined_with_sequentials.coursestructure_retrieved_at
            and combined_with_sequentials.row_num = 1
    )

select
    {{ dbt_utils.generate_surrogate_key(["block_id", "retrieved_at"]) }} as content_block_pk,
    courserun_readable_id,
    block_index,
    block_id,
    parent_block_id,
    sequential_block_id,
    chapter_block_id,
    block_category,
    block_title,
    block_metadata,
    retrieved_at,
    is_latest
from combined_course_content
