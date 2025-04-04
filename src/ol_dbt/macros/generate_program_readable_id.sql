{% macro generate_micromasters_program_readable_id(micromasters_program_id, program_title) %}
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
