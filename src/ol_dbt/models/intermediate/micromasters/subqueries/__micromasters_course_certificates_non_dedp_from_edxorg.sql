with courserun_certificates as (
    select * from {{ ref('int__edxorg__mitx_courserun_certificates') }}
)

, edxorg_users as (
    select * from {{ ref('int__edxorg__mitx_users') }}
)

, micromasters_users as (
    select * from {{ ref('__micromasters__users') }}
)

, courseruns as (
    select * from {{ ref('int__edxorg__mitx_courseruns') }}
)

, micromasters_programs as (
    select * from {{ ref('int__micromasters__programs') }}
)

select
    micromasters_programs.program_title
    , micromasters_programs.program_id as micromasters_program_id
    , courseruns.courserun_title
    , courseruns.courserun_readable_id
    , '{{ var("edxorg") }}' as courserun_platform
    , courseruns.course_number
    , edxorg_users.user_username as user_edxorg_username
    , micromasters_users.user_mitxonline_username
    , edxorg_users.user_full_name
    , edxorg_users.user_country
    , edxorg_users.user_email
    , courserun_certificates.courseruncertificate_download_url
    , courserun_certificates.courseruncertificate_download_uuid
    , courserun_certificates.courseruncertificate_created_on
    , courserun_certificates.courseruncertificate_updated_on
from courserun_certificates
inner join courseruns on courserun_certificates.courserun_readable_id = courseruns.courserun_readable_id
inner join micromasters_programs on courseruns.micromasters_program_id = micromasters_programs.program_id
inner join edxorg_users on courserun_certificates.user_id = edxorg_users.user_id
left join micromasters_users on edxorg_users.user_username = micromasters_users.user_edxorg_username
where micromasters_programs.program_id != {{ var("dedp_micromasters_program_id") }}
