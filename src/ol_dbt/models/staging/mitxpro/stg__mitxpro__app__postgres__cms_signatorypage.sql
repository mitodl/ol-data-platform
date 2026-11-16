with source as (
    {{ live_raw_rows('raw__xpro__app__postgres__cms_signatorypage') }}
)

, cleaned as (
    select
        page_ptr_id as wagtail_page_id
        , name as cms_signatorypage_name
        , title_1 as cms_signatorypage_title_1
        , title_2 as cms_signatorypage_title_2
        , organization as cms_signatorypage_organization
    from source
)

select * from cleaned
