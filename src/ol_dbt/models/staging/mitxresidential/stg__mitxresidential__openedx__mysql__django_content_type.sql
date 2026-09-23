-- MITx Residential open edX Django content types, needed to resolve the generic foreign keys
-- (content_type_id, content_object_id) on the forum-v2 tables. The ids are assigned by
-- migration order and differ per deployment, so join on app_label/model, never on a literal id.

with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__mitx__openedx__mysql__django_content_type') }}
)

, cleaned as (

    select
        id as contenttype_id
        , app_label as contenttype_app_label
        , model as contenttype_model
    from source
)

select * from cleaned
