-- Program information for MITxPro

with programs as (
    select * from {{ ref('stg__mitxpro__app__postgres__courses_program') }}
)

, cms_programs as (
    select * from {{ ref('stg__mitxpro__app__postgres__cms_programpage') }}
)

select
    programs.program_id
    , programs.program_title
    , programs.program_is_live
    , programs.program_readable_id
    , programs.program_is_external
    , cms_programs.cms_programpage_description
    , cms_programs.cms_programpage_subhead
    , cms_programs.cms_programpage_catalog_details
    , cms_programs.cms_programpage_duration
    , cms_programs.cms_programpage_time_commitment

from programs
left join cms_programs
    on cms_programs.program_id = programs.program_id
