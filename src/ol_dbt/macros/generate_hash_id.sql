{% macro generate_hash_id(string) %}
    -- Be cautious about changing the hash function as it will impact the primary key used by Hightouch
    lower(
        to_hex(
            sha256(
                cast({{ string }} as varbinary)  -- noqa
            )
        )
    )
{% endmacro %}
