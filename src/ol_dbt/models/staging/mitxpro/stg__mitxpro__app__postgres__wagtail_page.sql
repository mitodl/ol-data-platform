with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__wagtailcore_page') }}
)

, cleaned as (
    select
        id as wagtail_page_id
        , path as wagtail_page_path
        , depth as wagtail_page_depth
        , numchild as wagtail_page_num_children
        , title as wagtail_page_title
        , slug as wagtail_page_slug
        , live as wagtail_page_is_live
        , has_unpublished_changes as wagtail_page_has_unpublished_changes
        , seo_title as wagtail_page_seo_title

        , url_path as wagtail_page_url_path
        , search_description as wagtail_page_search_description
        , content_type_id as contenttype_id
        , owner_id as owner_user_id
        ,{{ cast_timestamp_to_iso8601('latest_revision_created_at') }} as wagtail_page_latest_revision_created_on
        ,{{ cast_timestamp_to_iso8601('first_published_at') }} as wagtail_page_first_published_on
        , live_revision_id as wagtail_page_live_pagerevision_id
        ,{{ cast_timestamp_to_iso8601('last_published_at') }} as wagtail_page_last_published_on

    from source
)

select * from cleaned
