with websitecontents as (
    select * from {{ ref('stg__ocw__studio__postgres__websites_websitecontent') }}
)

, instructor_contents as (
    select
        websitecontent_text_id as course_instructor_uuid
        , websitecontent_title as course_instructor_title
        , json_query(websitecontent_metadata, 'lax $.first_name' omit quotes) as course_instructor_first_name
        , json_query(websitecontent_metadata, 'lax $.last_name' omit quotes) as course_instructor_last_name
        , json_query(websitecontent_metadata, 'lax $.middle_initial' omit quotes) as course_instructor_middle_initial
        , json_query(websitecontent_metadata, 'lax $.salutation' omit quotes) as course_instructor_salutation
    from websitecontents
    where websitecontent_type = 'instructor'
)

, unnested_instructor_contents as (
    select
        websitecontents.website_uuid as course_uuid
        , course_instructor_uuid  -- noqa
    from websitecontents
    cross join
        unnest(cast(json_parse(course_instructor_uuids) as array (varchar))) as t(course_instructor_uuid) -- noqa
    where websitecontents.websitecontent_type = 'sitemetadata'
)

select
    unnested_instructor_contents.course_uuid
    , unnested_instructor_contents.course_instructor_uuid
    , instructor_contents.course_instructor_title
    , instructor_contents.course_instructor_first_name
    , instructor_contents.course_instructor_last_name
    , instructor_contents.course_instructor_salutation
    , instructor_contents.course_instructor_middle_initial
from unnested_instructor_contents
inner join instructor_contents
    on unnested_instructor_contents.course_instructor_uuid = instructor_contents.course_instructor_uuid
