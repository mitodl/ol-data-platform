with source as (
    select * from {{ source('ol_warehouse_raw_data', 'raw__ocw__s3__course_content') }}
)

-- The load appends a full set of rows each time a course changes, all stamped
-- with one course_retrieved_at. The newest set is the course: a page removed
-- from it is still in the older sets, so deduplicating per row would keep it.
, newest as (
    select
        course_slug
        , max(course_retrieved_at) as course_retrieved_at
    from source
    group by course_slug
)

-- A course is read whole again to retry a file Tika failed on, and a file
-- that extracted before can fail on the retry. The file has not changed while
-- its ETag has not, so its last extracted text still stands. Only the files
-- failed in a newest set are looked up, so the cost does not grow with the
-- table's history.
, failed as (
    select
        source.s3_key
        , source.file_etag
    from source
    inner join newest
        on
            source.course_slug = newest.course_slug
            and source.course_retrieved_at = newest.course_retrieved_at
    where source.extraction_status = 'failed'
)

, extracted as (
    select
        source.s3_key
        , source.file_etag
        , source.content
        , row_number() over (
            partition by source.s3_key, source.file_etag
            order by source.course_retrieved_at desc
        ) as read_rank
    from source
    inner join failed
        on
            source.s3_key = failed.s3_key
            and source.file_etag = failed.file_etag
    where source.extraction_status = 'extracted'
)

select
    source.course_slug
    , source.content_kind as coursecontent_kind
    , source.s3_key as coursecontent_s3_key
    , {{ json_extract_scalar('source.data_json', "'$.title'") }} as coursecontent_title
    , {{ json_extract_scalar('source.data_json', "'$.description'") }} as coursecontent_description
    , {{ json_extract_scalar('source.data_json', "'$.content'") }} as coursecontent_body
    , {{ json_extract_scalar('source.data_json', "'$.resourcetype'") }} as coursecontent_resource_type
    , {{ json_extract_scalar('source.data_json', "'$.resource_type'") }} as coursecontent_legacy_resource_type
    , {{ json_extract_scalar('source.data_json', "'$.file'") }} as coursecontent_file
    , {{ json_extract_scalar('source.data_json', "'$.file_type'") }} as coursecontent_file_type
    , {{ json_array_string('source.data_json', "'$.learning_resource_types'") }}
        as coursecontent_learning_resource_types
    , {{ json_extract_scalar('source.data_json', "'$.video_metadata.youtube_id'") }}
        as coursecontent_video_youtube_id
    , {{ json_extract_scalar('source.data_json', "'$.video_files.archive_url'") }}
        as coursecontent_video_archive_url
    , {{ json_extract_scalar('source.data_json', "'$.video_files.video_transcript_file'") }}
        as coursecontent_video_transcript_file
    , {{ json_extract_scalar('source.data_json', "'$.transcript_file'") }} as coursecontent_legacy_transcript_file
    , {{ json_extract_scalar('source.data_json', "'$.thumbnail_file'") }} as coursecontent_legacy_thumbnail_file
    , {{ json_extract_scalar('source.data_json', "'$.site_uid'") }} as course_site_uid
    , {{ json_extract_scalar('source.data_json', "'$.legacy_uid'") }} as course_legacy_uid
    , case when source.content_kind = 'course' then source.data_json end as course_data_json
    , source.file_key as coursecontent_text_file_key
    , coalesce(extracted.content, source.content) as coursecontent_text
    , case when extracted.content is not null then 'extracted' else source.extraction_status end
        as coursecontent_text_extraction_status
    , {{ cast_timestamp_to_iso8601('source.course_retrieved_at') }} as coursecontent_retrieved_on
from source
inner join newest
    on
        source.course_slug = newest.course_slug
        and source.course_retrieved_at = newest.course_retrieved_at
left join extracted
    on
        source.extraction_status = 'failed'
        and source.s3_key = extracted.s3_key
        and source.file_etag = extracted.file_etag
        and extracted.read_rank = 1
-- A course that left the bucket loads one "unpublished" row so that it becomes
-- the newest set; the row itself is not content.
where source.content_kind != 'unpublished'
