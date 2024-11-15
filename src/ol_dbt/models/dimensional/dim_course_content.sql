with mitxonline_course_structure as (
    select * from {{ ref('stg__mitxonline__openedx__api__course_structure') }}
    order by courserun_readable_id, coursestructure_retrieved_at, coursestructure_block_index
)

, edxorg_course_structure as (
    select * from {{ ref('stg__edxorg__s3__course_structure') }}
    order by courserun_readable_id, coursestructure_retrieved_at, coursestructure_block_index
)

, xpro_course_structure as (
    select * from {{ ref('stg__mitxpro__openedx__api__course_structure') }}
    order by courserun_readable_id, coursestructure_retrieved_at, coursestructure_block_index
)

, residential_course_structure as (
    select * from {{ ref('stg__mitxresidential__openedx__api__course_structure') }}
    order by courserun_readable_id, coursestructure_retrieved_at, coursestructure_block_index
)

, combined as (
    select
        {{ generate_hash_id('mitxonline') }} as platform_id
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
         {{ generate_hash_id('edxorg') }} as platform_id
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
         {{ generate_hash_id('mitxpro') }} as platform_id
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
        {{ generate_hash_id('residential') }} as platform_id
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

select
    {{ generate_hash_id('courserun_readable_id || coursestructure_block_id || coursestructure_retrieved_at') }} as content_id
    , platform_id
    , courserun_readable_id
    , coursestructure_block_index as content_block_index
    , coursestructure_block_id as content_block_id
    , coursestructure_parent_block_id as parent_content_block_id
    , coursestructure_block_category as content_block_category
    , coursestructure_block_title as content_block_title
    , coursestructure_block_metadata as content_block_metadata
    , coursestructure_retrieved_at as retrieved_at

from combined
