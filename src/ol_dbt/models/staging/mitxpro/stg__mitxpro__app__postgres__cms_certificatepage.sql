{# The Postgres column is "CEUs". Airbyte (production) writes it as ceus, dlt (QA, and
   production after its cutover) snake-cases it to ce_us, and reflects numeric(5,2) as a
   decimal where Airbyte wrote a double. #}
{% set raw_certificate_pages = source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__cms_certificatepage') %}
{% set ceus_column = 'ceus' if not execute
    or 'ceus' in adapter.get_columns_in_relation(raw_certificate_pages) | map(attribute='name') | map('lower')
    else 'ce_us' %}

with source as (
    select * from {{ raw_certificate_pages }}
)

{{ deduplicate_raw_table(raw_table='raw__xpro__app__postgres__cms_certificatepage', partition_columns='page_ptr_id') }}
, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , product_name as cms_certificate_product_name
        , cast({{ ceus_column }} as double) as cms_certificate_ceus
        , {{ json_array_field_values('signatories', 'value', 'integer') }}
            as cms_certificate_signitory_ids
        , institute_text as cms_certificate_institute_text
        , overrides as cms_certificate_overrides
    from most_recent_source
)

select * from cleaned
