{% macro generate_micromasters_program_readable_id(micromasters_program_id, program_title) %}
    {{ adapter.dispatch('generate_micromasters_program_readable_id', 'open_learning')(micromasters_program_id, program_title) }}
{% endmacro %}

{% macro default__generate_micromasters_program_readable_id(micromasters_program_id, program_title) %}
 -- Generate readable IDs for MicroMasters programs that runs on the edx platform
    --- This is needed for programs that have not migrated to MITx Online yet
    case
        when {{ micromasters_program_id }} = 4
             --- Statistics and Data Science has multiple tracks, we need to extract the track name and add it to
             --- the readable id
             then 'program-v1:MITx+SDS+' || replace(regexp_extract({{ program_title }}, '\((.*?)\)', 1),' ', '-')
        when {{ micromasters_program_id }} = 5
             --- Finance (active) and MIT Finance (retired) has the same readable ID
             then 'program-v1:MITx+FIN'
         when {{ micromasters_program_id }} is not null then
              'program-v1:MITx+' || upper(array_join(
                  transform(split({{ program_title }}, ' '), -- noqa: PRS
                    x -> substring(x, 1, 1)
                  ), ''
                ))
    end
{% endmacro %}

{% macro duckdb__generate_micromasters_program_readable_id(micromasters_program_id, program_title) %}
    -- DuckDB-compatible version using list_transform / string_split / list_aggregate
    case
        when {{ micromasters_program_id }} = 4
             then 'program-v1:MITx+SDS+' || replace(regexp_extract({{ program_title }}, '\((.*?)\)', 1),' ', '-')
        when {{ micromasters_program_id }} = 5
             then 'program-v1:MITx+FIN'
        when {{ micromasters_program_id }} is not null then
              'program-v1:MITx+' || upper(
                  list_aggregate(
                      list_transform(string_split({{ program_title }}, ' '), x -> left(x, 1)),
                      'string_agg', ''
                  )
              )
    end
{% endmacro %}
