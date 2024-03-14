with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__wagtailcore_pagerevision') }}
)

, cleaned as (
    select
        id as wagtail_pagerevision_id
        , {{ cast_timestamp_to_iso8601('created_at') }} as wagtail_pagerevision_created_on
        , content_json as wagtail_pagerevision_content_json
        , page_id as wagtail_page_id
        , user_id

    from source
)

select * from cleaned
