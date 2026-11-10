{% macro generate_hash_id(string) %}
  {{ return(adapter.dispatch('generate_hash_id', 'open_learning')(string)) }}
{% endmacro %}

{% macro default__generate_hash_id(string) %}
    -- Be cautious about changing the hash function: program_certificate_hashed_id is the primary key of MIT Learn's ProgramCertificate (record_hash)
   lower(
       to_hex(
          sha256(
                cast({{ string }} as varbinary) --noqa
          )
       )
    )
{% endmacro %}

{% macro starrocks__generate_hash_id(string) %}
    {# sha2 takes the string and returns lowercase hex, the same digest as the default body #}
    sha2(cast({{ string }} as varchar), 256)
{% endmacro %}
