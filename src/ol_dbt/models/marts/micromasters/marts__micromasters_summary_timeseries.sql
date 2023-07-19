with enrollments as (
    select *
    from {{ ref('int__micromasters__course_enrollments') }}
)

, course_certificates as (
    select *
    from {{ ref('int__micromasters__course_certificates') }}
)

, program_certificates as (
    select *
    from {{ ref('int__micromasters__program_certificates') }}
)

, timeseries as (
    select end_date
    from unnest(
        sequence(date '2014-01-01', current_date, interval '1' month) || current_date
    ) as t(end_date) -- noqa
)

, enrollments_by_program as (
    select
        timeseries.end_date
        , enrollments.micromasters_program_id
        , enrollments.mitxonline_program_id
        , arbitrary(enrollments.program_title) as program_title
        , count(*) as total_enrollments
        , count(distinct enrollments.user_email) as unique_users
        , count(distinct enrollments.user_address_country) as unique_countries
        , count_if(enrollments.courserunenrollment_enrollment_mode = 'verified') as verified_enrollments
        , count(
            distinct case
                when enrollments.courserunenrollment_enrollment_mode = 'verified' then enrollments.user_email
            end
        )
            as unique_verified_users
    from enrollments
    inner join timeseries on from_iso8601_timestamp(enrollments.courserunenrollment_created_on) < timeseries.end_date
    group by enrollments.micromasters_program_id, enrollments.mitxonline_program_id, timeseries.end_date
)

, enrollments_total as (
    select
        timeseries.end_date
        , 'total' as program_title
        , 0 as micromasters_program_id
        , 0 as mitxonline_program_id
        , count(
            distinct concat_ws(
                ',', cast(enrollments.user_id as varchar), enrollments.platform, enrollments.courserun_readable_id
            )
        )
            as total_enrollments
        , count(distinct enrollments.user_email) as unique_users
        , count(distinct enrollments.user_address_country) as unique_countries
        , count_if(enrollments.courserunenrollment_enrollment_mode = 'verified') as verified_enrollments
        , count(
            distinct case
                when enrollments.courserunenrollment_enrollment_mode = 'verified' then enrollments.user_email
            end
        )
            as unique_verified_users
    from enrollments
    inner join timeseries on from_iso8601_timestamp(enrollments.courserunenrollment_created_on) < timeseries.end_date
    group by timeseries.end_date
)

, enrollments_combined as (
    select
        end_date
        , program_title
        , micromasters_program_id
        , mitxonline_program_id
        , total_enrollments
        , unique_users
        , unique_countries
        , verified_enrollments
        , unique_verified_users
    from enrollments_by_program
    union all
    select
        end_date
        , program_title
        , micromasters_program_id
        , mitxonline_program_id
        , total_enrollments
        , unique_users
        , unique_countries
        , verified_enrollments
        , unique_verified_users
    from enrollments_total
)

, course_certificates_by_program as (
    select
        timeseries.end_date
        , course_certificates.micromasters_program_id
        , course_certificates.mitxonline_program_id
        , count(*) as course_certificates
        , count(distinct course_certificates.user_email) as unique_course_certificate_earners
    from course_certificates
    inner join
        timeseries
        on from_iso8601_timestamp(course_certificates.courseruncertificate_created_on) < timeseries.end_date
    group by course_certificates.micromasters_program_id, course_certificates.mitxonline_program_id, timeseries.end_date
)

, course_certificates_total as (
    select
        timeseries.end_date
        , 0 as micromasters_program_id
        , 0 as mitxonline_program_id
        , count(
            distinct concat_ws(
                ','
                , course_certificates.courserun_readable_id
                , course_certificates.user_edxorg_username
                , course_certificates.user_mitxonline_username
            )
        )
            as course_certificates
        , count(distinct course_certificates.user_email) as unique_course_certificate_earners
    from course_certificates
    inner join
        timeseries
        on from_iso8601_timestamp(course_certificates.courseruncertificate_created_on) < timeseries.end_date
    group by timeseries.end_date
)

, course_certificates_combined as (
    select
        end_date
        , micromasters_program_id
        , mitxonline_program_id
        , course_certificates
        , unique_course_certificate_earners
    from course_certificates_by_program
    union all
    select
        end_date
        , micromasters_program_id
        , mitxonline_program_id
        , course_certificates
        , unique_course_certificate_earners
    from course_certificates_total
)

, program_certificates_by_program as (
    select
        timeseries.end_date
        , program_certificates.micromasters_program_id
        , program_certificates.mitxonline_program_id
        , count(*) as program_certificates
    from program_certificates
    inner join
        timeseries
        on from_iso8601_timestamp(program_certificates.program_completion_timestamp) < timeseries.end_date
    group by
        program_certificates.micromasters_program_id, program_certificates.mitxonline_program_id, timeseries.end_date
)

, program_certificates_total as (
    select
        timeseries.end_date
        , 0 as micromasters_program_id
        , 0 as mitxonline_program_id
        , count(*) as program_certificates
    from program_certificates
    inner join
        timeseries
        on from_iso8601_timestamp(program_certificates.program_completion_timestamp) < timeseries.end_date
    group by timeseries.end_date
)

, program_certificates_combined as (
    select
        end_date
        , micromasters_program_id
        , mitxonline_program_id
        , program_certificates
    from program_certificates_by_program
    union all
    select
        end_date
        , micromasters_program_id
        , mitxonline_program_id
        , program_certificates
    from program_certificates_total
)


select
    enrollments_combined.program_title
    , enrollments_combined.total_enrollments
    , enrollments_combined.unique_users
    , enrollments_combined.unique_countries
    , enrollments_combined.verified_enrollments
    , enrollments_combined.unique_verified_users
    , course_certificates_combined.course_certificates
    , course_certificates_combined.unique_course_certificate_earners
    , program_certificates_combined.program_certificates
    , to_iso8601(enrollments_combined.end_date) as end_date
from enrollments_combined
left join course_certificates_combined
    on
        (
            enrollments_combined.micromasters_program_id = course_certificates_combined.micromasters_program_id
            or enrollments_combined.mitxonline_program_id = course_certificates_combined.mitxonline_program_id
        )
        and enrollments_combined.end_date = course_certificates_combined.end_date
left join program_certificates_combined
    on
        (
            enrollments_combined.micromasters_program_id = program_certificates_combined.micromasters_program_id
            or enrollments_combined.mitxonline_program_id = program_certificates_combined.mitxonline_program_id
        )
        and enrollments_combined.end_date = program_certificates_combined.end_date
order by
    enrollments_combined.micromasters_program_id
    , enrollments_combined.mitxonline_program_id
    , to_iso8601(enrollments_combined.end_date)
