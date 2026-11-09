{% macro join_verified_course_enrollments(enrollments) -%}
    {#
      Left joins tfact_enrollment's verified course enrollments onto `enrollments`, an alias
      for a model that carries platform, courserunenrollment_id, user_id and
      courserun_readable_id, so a verified count reads tfact_enrollment.is_verified
      (as verified_course_enrollments.is_verified) and does not restate its rule. The column
      is null for an enrollment that is not verified.
      These models hold the platform's display name where the fact holds its code, and an
      edX.org enrollment has no id of its own: the fact keys it on the learner and run.
    #}
    left join (
        select
            case platform
                when 'mitxonline' then '{{ var("mitxonline") }}'
                when 'edxorg' then '{{ var("edxorg") }}'
                when 'mitxpro' then '{{ var("mitxpro") }}'
                when 'residential' then '{{ var("residential") }}'
            end as platform
            , enrollment_id
            , is_verified
        from {{ ref('tfact_enrollment') }}
        where enrollment_type = 'course' and is_verified
    ) as verified_course_enrollments
        on {{ enrollments }}.platform = verified_course_enrollments.platform
        and case
            when {{ enrollments }}.platform = '{{ var("edxorg") }}'
                then {{ dbt_utils.generate_surrogate_key([
                    'cast(' ~ enrollments ~ '.user_id as varchar)',
                    enrollments ~ '.courserun_readable_id'
                ]) }}
            else cast({{ enrollments }}.courserunenrollment_id as varchar)
        end = verified_course_enrollments.enrollment_id
{%- endmacro %}
