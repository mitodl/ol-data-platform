-- Each course run's latest read by the courseware extractor: one row per scope part
-- it read, extracted or skipped, with the input it computed (courseware_scope_input).
-- The extraction asset creates courseware_scope_input on its first v2 write, so
-- until then there is no read to match and the model is empty.
{%- set stored = source('courseware_intermediate', 'courseware_scope_input') %}
{%- set stored_relation = load_relation(stored) if execute and model.resource_type != 'unit_test' else stored %}
with scope_inputs as (
    {%- if stored_relation is none %}
    select
        cast(null as {{ dbt.type_string() }}) as platform
        , cast(null as {{ dbt.type_string() }}) as courserun_readable_id
        , cast(null as {{ dbt.type_string() }}) as scope_id
        , cast(null as {{ dbt.type_bigint() }}) as scope_part
        , cast(null as {{ dbt.type_bigint() }}) as scope_part_count
        , cast(null as {{ dbt.type_string() }}) as input_text_checksum
        , cast(null as {{ dbt.type_string() }}) as header_version
        , cast(null as {{ dbt.type_timestamp() }}) as observed_at
    where false
    {%- else %}
    select
        *
        {{- missing_columns_as_nulls(stored, {'header_version': dbt.type_string()}) }}
    from {{ stored }}
    {%- endif %}
)

, latest_reads as (
    select
        platform
        , courserun_readable_id
        , max(observed_at) as observed_at
    from scope_inputs
    group by platform, courserun_readable_id
)

select
    scope_inputs.platform
    , scope_inputs.courserun_readable_id
    , scope_inputs.scope_id
    , scope_inputs.scope_part
    , scope_inputs.scope_part_count
    , scope_inputs.input_text_checksum
    , scope_inputs.header_version
    , scope_inputs.observed_at
from scope_inputs
inner join latest_reads
    on
        scope_inputs.platform = latest_reads.platform
        and scope_inputs.courserun_readable_id = latest_reads.courserun_readable_id
        and scope_inputs.observed_at = latest_reads.observed_at
