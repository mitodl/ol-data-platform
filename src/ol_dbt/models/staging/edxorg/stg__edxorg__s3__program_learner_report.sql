with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__edxorg__program_learner_report') }}
)

, source_sorted as (
    select
        *
        , case
            when "date program certificate awarded" = 'null' then null
            else to_iso8601(date_parse("date program certificate awarded", '%Y-%m-%dT%H:%i:%sZ'))
        end as program_certificate_awarded_dt
        , row_number() over (
            partition by "user id", "course run key", "program uuid"
            order by _airbyte_emitted_at desc, _ab_source_file_last_modified desc
        ) as row_num
    from source
)

, add_program_cert_latest as (
    select
        cast("user id" as integer) as user_id
        , "program uuid" as program_uuid
        , max(program_certificate_awarded_dt) as latest_program_cert_award_on
        , max("completed program") as ever_completed_program
    from source_sorted
    group by 1, 2
)

, dedup_source as (
    select * from source_sorted
    where row_num = 1
)

, cleaned as (

    select
        "authoring institution" as org_id
        , "program type" as program_type
        , "program uuid" as program_uuid
        , username as user_username
        , name as user_full_name
        , "course run key" as courserun_readable_id
        , "course title" as course_title
        , track as courserunenrollment_enrollment_mode
        , cast("user id" as integer) as user_id
        , cast(completed as boolean) as user_has_completed_course
        , cast("completed program" as boolean) as user_has_completed_program
        , cast("currently enrolled" as boolean) as courserunenrollment_is_active
        , cast("purchased as bundle" as boolean) as user_has_purchased_as_bundle
        , if("user roles" = 'null', null, "user roles") as user_roles
        , if("letter grade" = 'null', null, "letter grade") as courserungrade_letter_grade
        , if(grade = 'null', null, grade) as courserungrade_grade
        , case
            when "program uuid" like '%941d3eaf56966c7' then 'Finance'
            when "program uuid" like '%3173ff51e11a748' then 'MIT Finance'
            when "program uuid" like '%8c11bfd9c0d7b07' then 'Statistics and Data Science (General track)'
            when "program uuid" like '%cd7c6461dd9b1d4' then 'Statistics and Data Science (Social Sciences Track)'
            else "program title"
        end as program_title
        , to_iso8601(date_parse("course run start date", '%Y-%m-%d %H:%i:%s Z')) as courserun_start_on
        , to_iso8601(date_parse("date first enrolled", '%Y-%m-%d %H:%i:%s Z')) as courserunenrollment_created_on
        , case
            when "date completed" = 'null' then null
            else to_iso8601(date_parse("date completed", '%Y-%m-%d %H:%i:%s Z'))
        end as completed_course_on
        , case
            when "last activity date" = 'null' then null
            else {{ cast_date_to_iso8601('"last activity date"') }}
        end as courseactivity_last_activity_date
        , case
            when "date last unenrolled" = 'null' then null
            else to_iso8601(date_parse("date last unenrolled", '%Y-%m-%d %H:%i:%s Z'))
        end as courserunenrollment_unenrolled_on
        , case
            when "date first upgraded to verified" = 'null' then null
            else to_iso8601(date_parse("date first upgraded to verified", '%Y-%m-%d %H:%i:%s Z'))
        end as courserunenrollment_upgraded_on
        , program_certificate_awarded_dt as program_certificate_awarded_on
    from dedup_source

)

select
    cleaned.org_id
    , cleaned.program_type
    , cleaned.program_uuid
    , cleaned.program_title
    , cleaned.user_username
    , cleaned.user_full_name
    , cleaned.courserun_readable_id
    , cleaned.course_title
    , cleaned.courserunenrollment_enrollment_mode
    , cleaned.user_id
    , cleaned.user_has_completed_course
    , cast(add_program_cert_latest.ever_completed_program as boolean) as user_has_completed_program
    , cleaned.courserunenrollment_is_active
    , cleaned.user_has_purchased_as_bundle
    , cleaned.user_roles
    , cleaned.courserungrade_letter_grade
    , cleaned.courserungrade_grade
    , cleaned.courserun_start_on
    , cleaned.courserunenrollment_created_on
    , cleaned.completed_course_on
    , cleaned.courseactivity_last_activity_date
    , cleaned.courserunenrollment_unenrolled_on
    , cleaned.courserunenrollment_upgraded_on
    , add_program_cert_latest.latest_program_cert_award_on as program_certificate_awarded_on
    , regexp_extract(cleaned.program_title, '\((.*?)\)', 1) as program_track
from cleaned
left join add_program_cert_latest
    on
        cleaned.user_id = add_program_cert_latest.user_id
        and cleaned.program_uuid = add_program_cert_latest.program_uuid
