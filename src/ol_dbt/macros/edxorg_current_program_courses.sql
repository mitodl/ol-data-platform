{% macro edxorg_current_program_courses() -%}
    {#
      The edX program courses from the latest programs extraction, as a query to
      select from. Keyed on the programs' retrieved_at rather than the courses' own:
      if one stream's sync lands without the other's, programs are left with no
      courses (which the delivery refuses to send) instead of being paired with
      another day's. Every model that reads program courses for MIT Learn's edX
      programs goes through this so they all agree on the extraction.
    #}
    select *
    from {{ ref('stg__edxorg__s3__program_courses') }}
    where program_course_retrieved_at = (
        select max(program_retrieved_at) from {{ ref('stg__edxorg__s3__programs') }}
    )
{%- endmacro %}
