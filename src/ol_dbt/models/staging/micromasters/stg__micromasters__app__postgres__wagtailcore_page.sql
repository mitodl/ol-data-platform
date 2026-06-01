with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__micromasters__app__postgres__wagtailcore_page') }}
)

, cleaned as (
    select
        id                                                          as wagtail_page_id
        , slug                                                      as wagtail_page_slug
        , url_path                                                  as wagtail_page_url_path
        , title                                                     as wagtail_page_title
        , live                                                      as wagtail_page_is_live
        , depth                                                     as wagtail_page_depth
        , path                                                      as wagtail_page_path
        , seo_title                                                 as wagtail_page_seo_title
        , search_description                                        as wagtail_page_search_description
        , has_unpublished_changes                                   as wagtail_page_has_unpublished_changes
        , content_type_id
        , owner_id
        , if(
            live
            , concat('{{ var("micromasters_production_url") }}', slug, '/')
            , null
        )                                                           as wagtail_page_live_url
        , {{ cast_timestamp_to_iso8601('first_published_at') }}     as wagtail_page_first_published_on
        , {{ cast_timestamp_to_iso8601('last_published_at') }}      as wagtail_page_last_published_on
        , {{ cast_timestamp_to_iso8601('latest_revision_created_at') }} as wagtail_page_latest_revision_created_on
    from source
)

select * from cleaned
