with
    edxorg_course_xml_blocks as (select * from {{ ref('stg__edxorg__s3__course_xml_blocks') }})

    , openedx_course_xml_blocks as (select * from {{ ref('stg__openedx__s3__course_xml_blocks') }})

    , combined as (
        select * from edxorg_course_xml_blocks
        union all
        select * from openedx_course_xml_blocks
    )

select *
from combined
