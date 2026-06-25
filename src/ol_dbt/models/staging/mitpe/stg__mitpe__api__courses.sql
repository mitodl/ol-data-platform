with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitpe__api__courses') }}
)

select
    -- MIT PE has no stable UUID; url is the stable identifier
    coalesce(url, title)                        as course_readable_id
    , title                                     as course_title
    , url                                       as course_url
    , description                               as course_description
    , image__src                                as course_image_src
    , image__alt                                as course_image_alt
    , topics                                    as course_topics_raw
from source
where url is not null
