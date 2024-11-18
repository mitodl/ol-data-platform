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

, combined_course_content as (
    select
        combined.platform
        , combined.courserun_readable_id
        , combined.coursestructure_block_index as block_index
        , combined.coursestructure_block_id as block_id
        , combined.coursestructure_parent_block_id as parent_block_id
        , combined.coursestructure_block_category as block_category
        , combined.coursestructure_block_title as block_title
        , combined.coursestructure_block_metadata as block_metadata
        , combined.coursestructure_retrieved_at as retrieved_at
        , if(latest_course_structure.max_retrieved_date is not null, true, false) as is_latest
    from combined
    left join latest_course_structure
        on
            combined.platform = latest_course_structure.platform
            and combined.courserun_readable_id = latest_course_structure.courserun_readable_id
            and combined.coursestructure_retrieved_at = latest_course_structure.max_retrieved_date

)

select
    {{ generate_hash_id('block_id || retrieved_at') }} as id
    , courserun_readable_id
    , block_index
    , block_id
    , parent_block_id
    , block_category
    , block_title
    , block_metadata
    , retrieved_at
    , is_latest
from combined_course_content
