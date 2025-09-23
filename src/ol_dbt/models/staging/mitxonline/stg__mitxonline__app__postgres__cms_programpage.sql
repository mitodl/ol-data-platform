with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__mitxonline__app__postgres__cms_programpage") }}),
    cleaned as (
        select
            page_ptr_id as wagtail_page_id,
            program_id,
            description as program_description,
            length as program_length,
            effort as program_effort,
            prerequisites as program_prerequisites,
            faq_url as program_faq_url,
            about as program_about,
            what_you_learn as program_what_you_learn,
            video_url as program_video_url,
            json_query(price, 'lax $.value.text' omit quotes) as program_price
        from source
    )

select *
from cleaned
