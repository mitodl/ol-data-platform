{#
  One row per page or resource of an OCW course that MIT Learn loads as a
  ContentFile, derived the way Learn's OCW ETL does (transform_page,
  transform_contentfile and transform_contentfile_legacy in
  learning_resources/etl/ocw.py), from the same data.json files in the OCW live
  bucket.

  Learn drops a resource that has no file to read text from. For a video that
  file is its transcript (get_video_transcript_path): the first English entry of
  video_files.video_transcript_resources that names a file, else the first entry
  that names a file, else video_files.video_transcript_file. Learn raises, and
  so drops the resource, on an entry of the list that is not an object, on a
  language that is not a string, and on a chosen file that is not a string. A
  resource whose file is not in the bucket is dropped as well: Learn's read of
  it raises.

  Not reproduced: Learn passes a resource's description through nh3 with an
  allowlist of tags. The description here is the HTML OCW published.
#}

{% set ocw_base_url = 'https://ocw.mit.edu/' %}
{% set extension_pattern = "'\\.[^./]+$'" %}

with content as (
    select * from {{ ref('stg__ocw__s3__course_content') }}
)

, courses as (
    select
        course_slug
        , courserun_readable_id
    from {{ ref('int__ocw__live_courses') }}
)

, pages as (
    select
        course_slug
        , coursecontent_s3_key
        , 'page' as content_type
        , coursecontent_title as title
        , coursecontent_description as description
        , coursecontent_body as content
        , cast(null as varchar) as file_type
        , cast(null as varchar) as file_extension
        , cast(null as varchar) as image_src
        , cast(null as varchar) as youtube_id
        , coursecontent_learning_resource_types as content_tags
        , cast(null as varchar) as extraction_status
        , coursecontent_retrieved_on as retrieved_on
    from content
    where coursecontent_kind = 'page'
)

, resources as (
    select
        *
        -- data.json written before resourcetype existed names it resource_type.
        , coalesce(coursecontent_resource_type, '') != '' as is_current_format
        , coalesce(nullif(coursecontent_resource_type, ''), coursecontent_legacy_resource_type, '') = 'Video'
            as is_video
        -- Learn raises on a video_transcript_resources that is neither empty nor
        -- a list, which drops the resource.
        , coalesce(coursecontent_video_transcript_resources, 'null') not in ('null', '""', '{}', 'false', '0')
        and {{ try_parse_json_array('coursecontent_video_transcript_resources') }} is null
            as has_unreadable_transcripts
    from content
    where coursecontent_kind = 'resource'
)

-- One row per entry of a video's video_transcript_resources, with the JSON text
-- of its file and language.
, transcript_entries as (
    select
        resources.coursecontent_s3_key
        , transcript.idx
        , coalesce({{ json_is_object('transcript.element') }}, false) as is_object
        , {{ json_array_string('transcript.element', "'$.file'") }} as file_json
        , {{ json_array_string('transcript.element', "'$.language'") }} as language_json
        , {{ json_extract_scalar('transcript.element', "'$.file'") }} as transcript_file
        , {{ json_extract_scalar('transcript.element', "'$.language'") }} as transcript_language
    from resources
    cross join
        unnest({{ try_parse_json_array('resources.coursecontent_video_transcript_resources') }})  -- noqa: PRS
        with ordinality as transcript (element, idx)
    where
        resources.is_video
        and resources.is_current_format
)

-- Learn tests the file and language the way Python does: null, false, 0, "",
-- [] and {} are false.
, transcript_flags as (
    select
        *
        , coalesce(
            file_json not in ('null', 'false', '""', '[]', '{}')
            and coalesce({{ try_cast('file_json', 'double') }}, 1) != 0
            , false
        ) as names_file
        , coalesce(
            language_json not in ('null', 'false', '""', '[]', '{}')
            and coalesce({{ try_cast('language_json', 'double') }}, 1) != 0
            , false
        ) as names_language
    from transcript_entries
)

-- The entries that name a file, ranked the way Learn picks one.
, transcript_resources as (
    select
        coursecontent_s3_key
        , transcript_file
        , substr(file_json, 1, 1) = '"' as is_file_string
        , row_number() over (
            partition by coursecontent_s3_key
            order by
                case when lower(coalesce(transcript_language, '')) like 'en%' then 0 else 1 end
                , idx
        ) as transcript_rank
    from transcript_flags
    where names_file
)

-- Learn calls .get on every entry and .lower on the language of every entry
-- that names a file, so an entry that is not an object or a language that is
-- not a string raises, which drops the resource.
, unreadable_transcript_entries as (
    select distinct coursecontent_s3_key
    from transcript_flags
    where
        not is_object
        or (names_file and names_language and substr(language_json, 1, 1) != '"')
)

, resource_files as (
    select
        resources.*
        , case
            when
                resources.is_video
                and resources.is_current_format
                and (
                    resources.has_unreadable_transcripts
                    or unreadable_transcript_entries.coursecontent_s3_key is not null
                    -- Learn's .lstrip raises on a chosen file that is not a string.
                    or not coalesce(transcript_resources.is_file_string, true)
                )
                then null
            when resources.is_video and resources.is_current_format
                then coalesce(transcript_resources.transcript_file, resources.coursecontent_video_transcript_file)
            when resources.is_video then resources.coursecontent_legacy_transcript_file
            else resources.coursecontent_file
        end as text_file_path
        , case
            when resources.is_video
                then
                    coalesce(
                        nullif(resources.coursecontent_file, ''), nullif(resources.coursecontent_video_archive_url, ''), ''
                    )
            else coalesce(resources.coursecontent_file, '')
        end as extension_path
    from resources
    left join transcript_resources
        on
            resources.coursecontent_s3_key = transcript_resources.coursecontent_s3_key
            and transcript_resources.transcript_rank = 1
    left join unreadable_transcript_entries
        on resources.coursecontent_s3_key = unreadable_transcript_entries.coursecontent_s3_key
)

, resource_content_files as (
    select
        course_slug
        , coursecontent_s3_key
        , case
            when is_video then 'video'
            when coursecontent_file_type like 'video/%' then 'video'
            when coursecontent_file_type = 'application/pdf' then 'pdf'
            else 'file'
        end as content_type
        , coursecontent_title as title
        , case
            when is_current_format then coalesce(coursecontent_body, '')
            else coursecontent_description
        end as description
        , case when coursecontent_text_extraction_status = 'extracted' then coursecontent_text end as content
        , coursecontent_file_type as file_type
        , coalesce({{ regexp_extract_or_null('extension_path', extension_pattern) }}, '') as file_extension
        , case
            when not is_video then null
            when not is_current_format then nullif(coursecontent_legacy_thumbnail_file, '')
            when coalesce(coursecontent_video_youtube_id, '') != ''
                then {{ dbt.concat(["'https://i.ytimg.com/vi/'", "coursecontent_video_youtube_id", "'/hqdefault.jpg'"]) }}
        end as image_src
        , case
            when is_video and is_current_format then nullif(coursecontent_video_youtube_id, '')
        end as youtube_id
        , coursecontent_learning_resource_types as content_tags
        , coursecontent_text_extraction_status as extraction_status
        , coursecontent_retrieved_on as retrieved_on
    from resource_files
    where
        coalesce(coursecontent_title, '') not in ('3play caption file', '3play pdf file')
        and coalesce(text_file_path, '') like '%courses%'
        and coalesce(coursecontent_text_extraction_status, '') != 'missing'
)

, content_files as (
    select * from pages
    union all
    select * from resource_content_files
)

select
    courses.courserun_readable_id
    , content_files.course_slug
    -- The directory holding data.json, with its trailing slash.
    , substr(content_files.coursecontent_s3_key, 1, length(content_files.coursecontent_s3_key) - length('data.json'))
        as content_file_key
    , content_files.content_type
    , content_files.title
    , content_files.description
    , {{ dbt.concat([
        "'" ~ ocw_base_url ~ "'"
        , "substr(content_files.coursecontent_s3_key, 1, length(content_files.coursecontent_s3_key) - length('data.json'))"
    ]) }} as url
    , content_files.content
    , content_files.file_type
    , content_files.file_extension
    , content_files.image_src
    , content_files.youtube_id
    , content_files.content_tags
    , content_files.extraction_status
    , content_files.retrieved_on
from content_files
-- Learn skips a course with neither uid, and a ContentFile belongs to a run.
inner join courses
    on content_files.course_slug = courses.course_slug
where courses.courserun_readable_id is not null
