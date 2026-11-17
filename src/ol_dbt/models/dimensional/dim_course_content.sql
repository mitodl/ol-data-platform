with mitxonline_course_structure as (
    select * from {{ ref('stg__mitxonline__openedx__api__course_structure') }}
)

, edxorg_course_structure as (
    select * from {{ ref('stg__edxorg__s3__course_structure') }}
)

, xpro_course_structure as (
    select * from {{ ref('stg__mitxpro__openedx__api__course_structure') }}
)

, residential_course_structure as (
    select * from {{ ref('stg__mitxresidential__openedx__api__course_structure') }}
)

-- union all, not union distinct: in production (2026-11-17) no branch repeats a
-- (courserun_readable_id, block_id, retrieved_at), and the platform literal keeps branches apart, so the
-- distinct removed no rows. It made StarRocks group every row on every column, block_metadata JSON
-- included, which ran QA's CN out of memory. Staging only dedups a block per retrieval and course hash,
-- so two hashes in one retrieval would repeat it here; the content_block_pk unique test warns on that.
, combined as (
    select
        'mitxonline' as platform
        , courserun_readable_id
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
    from mitxonline_course_structure

    union all

    select
        'edxorg' as platform
        , courserun_readable_id
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
    from edxorg_course_structure

    union all

    select
        'mitxpro' as platform
        , courserun_readable_id
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
    from xpro_course_structure

    union all

    select
        'residential' as platform
        , courserun_readable_id
        , coursestructure_block_index
        , coursestructure_block_id
        , coursestructure_parent_block_id
        , coursestructure_block_category
        , coursestructure_block_title
        , coursestructure_block_metadata
        , coursestructure_retrieved_at
    from residential_course_structure
)

, latest_course_structure as (
    select
        platform
        , courserun_readable_id
        , max(coursestructure_retrieved_at) as max_retrieved_date
    from combined
    group by platform, courserun_readable_id
)

, combined_with_hierarchy as (
    select
        *
        , {{ last_value_ignore_nulls(
            "case when coursestructure_block_category = 'sequential' then coursestructure_block_id end"
          ) }} over (
            partition by platform, courserun_readable_id, coursestructure_retrieved_at
            order by coursestructure_block_index nulls last
            rows between unbounded preceding and current row
        ) as sequential_block_id
        , {{ last_value_ignore_nulls(
            "case when coursestructure_block_category = 'sequential' then coursestructure_block_title end"
          ) }} over (
            partition by platform, courserun_readable_id, coursestructure_retrieved_at
            order by coursestructure_block_index nulls last
            rows between unbounded preceding and current row
        ) as sequential_title
        , {{ last_value_ignore_nulls(
            "case when coursestructure_block_category = 'chapter' then coursestructure_block_id end"
          ) }} over (
            partition by platform, courserun_readable_id, coursestructure_retrieved_at
            order by coursestructure_block_index nulls last
            rows between unbounded preceding and current row
        ) as chapter_block_id
        , {{ last_value_ignore_nulls(
            "case when coursestructure_block_category = 'chapter' then coursestructure_block_title end"
          ) }} over (
            partition by platform, courserun_readable_id, coursestructure_retrieved_at
            order by coursestructure_block_index nulls last
            rows between unbounded preceding and current row
        ) as chapter_title
    from combined
)

, combined_course_content as (
    select
        combined_with_hierarchy.platform
        , combined_with_hierarchy.courserun_readable_id
        , combined_with_hierarchy.coursestructure_block_index as block_index
        , combined_with_hierarchy.coursestructure_block_id as block_id
        , combined_with_hierarchy.coursestructure_parent_block_id as parent_block_id
        , combined_with_hierarchy.coursestructure_block_category as block_category
        , combined_with_hierarchy.coursestructure_block_title as block_title
        , combined_with_hierarchy.coursestructure_block_metadata as block_metadata
        , combined_with_hierarchy.coursestructure_retrieved_at as retrieved_at
        , combined_with_hierarchy.chapter_block_id
        , combined_with_hierarchy.chapter_title
        , combined_with_hierarchy.sequential_block_id
        , combined_with_hierarchy.sequential_title
        , if(latest_course_structure.max_retrieved_date is not null, true, false) as is_latest
    from combined_with_hierarchy
    left join latest_course_structure
        on
            combined_with_hierarchy.platform = latest_course_structure.platform
            and combined_with_hierarchy.courserun_readable_id = latest_course_structure.courserun_readable_id
            and combined_with_hierarchy.coursestructure_retrieved_at = latest_course_structure.max_retrieved_date
)

select
    {{ dbt_utils.generate_surrogate_key(['block_id','retrieved_at']) }} as content_block_pk
    , platform
    , courserun_readable_id
    , block_index
    , block_id
    , parent_block_id
    , sequential_block_id
    , chapter_block_id
    , block_category
    , block_title
    , block_metadata
    , retrieved_at
    , is_latest
from combined_course_content
