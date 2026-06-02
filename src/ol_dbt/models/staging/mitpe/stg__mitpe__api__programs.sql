with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitpe__api__programs') }}
)

select
    coalesce(url, title)                        as program_readable_id
    , title                                     as program_title
    , url                                       as program_url
    , description                               as program_description
    , image__src                                as program_image_src
    , image__alt                                as program_image_alt
    , topics                                    as program_topics_raw
from source
where url is not null
