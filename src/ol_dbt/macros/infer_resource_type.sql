{% macro infer_resource_type(resource_url, resource_type) %}
    CASE
        -- Use existing resource_type if available
        WHEN {{ resource_type }} IS NOT NULL THEN {{ resource_type }}
        -- Infer from URL file extension
        WHEN LOWER({{ resource_url }}) LIKE '%.pdf'
          OR LOWER({{ resource_url }}) LIKE '%.docx'
          OR LOWER({{ resource_url }}) LIKE '%.doc'
          OR LOWER({{ resource_url }}) LIKE '%.txt'
          OR LOWER({{ resource_url }}) LIKE '%.rtf'
            THEN 'Document'
        WHEN LOWER({{ resource_url }}) LIKE '%.mp4'
          OR LOWER({{ resource_url }}) LIKE '%.mov'
          OR LOWER({{ resource_url }}) LIKE '%.avi'
          OR LOWER({{ resource_url }}) LIKE '%.webm'
          OR LOWER({{ resource_url }}) LIKE '%.m4v'
            THEN 'Video'
        WHEN LOWER({{ resource_url }}) LIKE '%.jpg'
          OR LOWER({{ resource_url }}) LIKE '%.jpeg'
          OR LOWER({{ resource_url }}) LIKE '%.png'
          OR LOWER({{ resource_url }}) LIKE '%.gif'
          OR LOWER({{ resource_url }}) LIKE '%.svg'
            THEN 'Image'
        WHEN LOWER({{ resource_url }}) LIKE '%.zip'
          OR LOWER({{ resource_url }}) LIKE '%.tar'
          OR LOWER({{ resource_url }}) LIKE '%.gz'
            THEN 'Archive'
        ELSE 'Other'
    END
{% endmacro %}
