with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__cms_certificatepage') }}
)

{{ deduplicate_raw_table(raw_table='raw__xpro__app__postgres__cms_certificatepage', partition_columns='page_ptr_id') }}
, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , product_name as cms_certificate_product_name
        , CEUs as cms_certificate_ceus --noqa
        -- the alias sits on its own line because the Trino body of the macro ends in a comment
        , {{ json_array_field_values('signatories', 'value', 'integer') }}
            as cms_certificate_signitory_ids
        , institute_text as cms_certificate_institute_text
        , overrides as cms_certificate_overrides
    from most_recent_source
)

select * from cleaned
