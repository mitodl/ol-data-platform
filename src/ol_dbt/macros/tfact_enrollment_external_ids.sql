{#
    tfact_enrollment's enrollment_id for the two sources with no native id. The model mints
    it from these, and tfact_enrollment_gone_from_source matches stored rows against them.
    Two definitions that drifted apart would make that predicate match every stored row.
#}
{% macro emeritus_enrollment_id(source_alias) -%}
    {{ dbt_utils.generate_surrogate_key([
        source_alias ~ '.courserun_external_readable_id',
        source_alias ~ '.user_email',
        source_alias ~ '.user_first_name',
        source_alias ~ '.user_last_name'
    ]) }}
{%- endmacro %}

{% macro global_alumni_enrollment_id(source_alias) -%}
    {{ dbt_utils.generate_surrogate_key([
        'coalesce(' ~ source_alias ~ '.user_email, cast(' ~ source_alias ~ '.user_id as varchar))',
        source_alias ~ '.courserun_external_readable_id'
    ]) }}
{%- endmacro %}

{#
    The stored Emeritus and Global Alumni rows whose key staging no longer has. Their keys
    hash learner details (for Emeritus, the name), so an edit at the source mints a new key
    and delete+insert never removes the old one. Sources with an enrollment id keep their
    key, so they are not checked. The platform list comes from staging, so a source with no
    rows there (a failed load) is skipped rather than emptied.
#}
{% macro tfact_enrollment_gone_from_source() -%}
    platform in (
        select distinct 'emeritus'
        from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }}
        union all
        select distinct 'global_alumni'
        from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }}
    )
    and enrollment_id not in (
        select {{ emeritus_enrollment_id('emeritus_enrollments') }}
        from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }} as emeritus_enrollments
        union all
        select {{ global_alumni_enrollment_id('global_alumni_enrollments') }}
        from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }} as global_alumni_enrollments
    )
{%- endmacro %}
