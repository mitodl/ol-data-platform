with courserun_certificates as (
    select * from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)

, courseruns as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, programs as (
    select * from {{ ref('int__mitx__programs') }}
)

select
    programs.program_title
    , programs.micromasters_program_id
    , programs.mitxonline_program_id
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , '{{ var("edxorg") }}' as courserun_platform
    , courseruns.course_number
    , courserun_certificates.user_username as user_edxorg_username
    , courserun_certificates.user_id as user_edxorg_id
    , courserun_certificates.courseruncertificate_download_url
    , courserun_certificates.courseruncertificate_download_uuid
    , courserun_certificates.courseruncertificate_created_on
    , courserun_certificates.courseruncertificate_updated_on
from courserun_certificates
inner join courseruns on courserun_certificates.courserun_readable_id = courseruns.courserun_readable_id
inner join programs on courseruns.micromasters_program_id = programs.micromasters_program_id
where programs.is_dedp_program = false
