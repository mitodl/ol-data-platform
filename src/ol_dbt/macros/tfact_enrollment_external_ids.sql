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
    hash details the source can edit, and delete+insert never removes the old key. An
    Emeritus run code change is what has done this on production so far; staging also drops
    a name the source has replaced. Sources with an enrollment id keep their key, so they
    are not checked. This has no guard, so assert_tfact_enrollment_has_no_keys_gone_from_source
    reports the rows a guarded delete left behind.
#}
{% macro tfact_enrollment_gone_from_source() -%}
    platform in ('emeritus', 'global_alumni')
    and enrollment_id not in (
        select {{ emeritus_enrollment_id('emeritus_enrollments') }}
        from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }} as emeritus_enrollments
        union all
        select {{ global_alumni_enrollment_id('global_alumni_enrollments') }}
        from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }} as global_alumni_enrollments
    )
{%- endmacro %}

{#
    tfact_enrollment's post_hook. A platform whose staging holds under
    min_staged_percent of the fact's rows is skipped with a warning and its rows stay. An
    Airbyte clear, a rebuilt connection or a dlt replace cutover cuts staging to the latest
    sync, and a key staging lost that way is not gone from the source. Reaching this floor
    is not normal: the most names one sync has replaced is 4, and the first run here deletes
    0.27% of Emeritus. A staging cut to the latest sync is short by about 6% of Emeritus and
    5% of Global Alumni (measured 2026-11-02). The skipped rows fail the singular test.
    Counted in a separate query so the DELETE doesn't read the table it deletes from.
#}
{% macro delete_tfact_enrollment_gone_from_source(min_staged_percent=99) -%}
    {%- set row_counts_query -%}
        select staged.platform, staged.staged_rows, stored.stored_rows
        from (
            select 'emeritus' as platform, count(*) as staged_rows
            from {{ ref('stg__emeritus__api__bigquery__user_enrollments') }}
            union all
            select 'global_alumni' as platform, count(*) as staged_rows
            from {{ ref('stg__global_alumni__api__bigquery__user_enrollments') }}
        ) as staged
        inner join (
            select platform, count(*) as stored_rows
            from {{ this }}
            where platform in ('emeritus', 'global_alumni')
            group by platform
        ) as stored
            on staged.platform = stored.platform
    {%- endset -%}
    {%- if execute -%}
        {%- set checked_platforms = [] -%}
        {%- for row in run_query(row_counts_query).rows -%}
            {%- if row['staged_rows'] * 100 >= row['stored_rows'] * min_staged_percent -%}
                {%- do checked_platforms.append(row['platform']) -%}
            {%- else -%}
                {%- do exceptions.warn(
                    "tfact_enrollment: skipped the delete for " ~ row['platform'] ~ ": staging has "
                    ~ row['staged_rows'] ~ " rows and the fact " ~ row['stored_rows'] ~ "."
                ) -%}
            {%- endif -%}
        {%- endfor -%}
        {%- if checked_platforms %}
            delete from {{ this }}
            where platform in ('{{ checked_platforms | join("', '") }}')
                and {{ tfact_enrollment_gone_from_source() }}
        {%- endif -%}
    {%- endif -%}
{%- endmacro %}
