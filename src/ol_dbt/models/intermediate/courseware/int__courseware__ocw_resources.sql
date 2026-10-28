{#
    The resources of the OCW courses var('courseware_ocw_courses') selects that
    int__courseware__extraction_sources may read: every file that is not a page, has
    text and is not an answer key. An answer key is a file int__courseware__ocw_files
    marks as one, a resource a table links in a column headed Solutions (or Answers,
    Key), or one whose text is the same as such a resource's. A table of its own for
    the reason int__courseware__ocw_files is.
#}
{% set ocw_transcript_extensions = ['.srt', '.vtt', '.webvtt'] %}
{# a table column of answer keys: "Solutions", "Exam Solutions", "Answer Key"; not "Problem Sets and Solutions" #}
{% set ocw_solution_column = "'^((problem sets?|psets?|exams?|quiz(zes)?|labs?|homeworks?|assignments?|practice exams?|exam review|sample|recitations?|midterms?|finals?|final exam) )?(solutions?|answers?|answer keys?|keys?|solns?)$'" %}

-- the header cells of each table's first row, and the columns among them that hold
-- answer keys
with ocw_table_headers as (
    select
        courserun_readable_id
        , page_key
        , table_index
        , {{ regexp_extract_all('row_html', courseware_ocw_table_cell(), 1) }} as header_cells
    from {{ ref('int__courseware__ocw_table_rows') }}
    where row_index = 2
)

, ocw_table_columns as (
    select
        headers.courserun_readable_id
        , headers.page_key
        , headers.table_index
        , {{ array_length('headers.header_cells') }} as column_count
        , columns.column_index
        , lower({{ courseware_html_text(element_at_array('headers.header_cells', 'columns.column_index')) }})
            as header_text
    from ocw_table_headers as headers
    cross join {{ unnest_sequence(array_length('headers.header_cells'), 'columns', 'column_index') }}
    where {{ array_length('headers.header_cells') }} > 0
)

-- a row whose cells line up with the header's; rows spanning columns are left out
, ocw_solution_cells as (
    select
        table_rows.courserun_readable_id
        , {{ regexp_extract_all(
            element_at_array(regexp_extract_all('table_rows.row_html', courseware_ocw_table_cell(), 1), 'columns.column_index')
            , courseware_ocw_resource_href()
            , 1
        ) }} as hrefs
    from {{ ref('int__courseware__ocw_table_rows') }} as table_rows
    inner join ocw_table_columns as columns
        on
            table_rows.page_key = columns.page_key
            and table_rows.table_index = columns.table_index
    where
        table_rows.row_index > 2
        and {{ regexp_like("replace(columns.header_text, '&amp;', '&')", ocw_solution_column) }}
        and {{ array_length(regexp_extract_all('table_rows.row_html', courseware_ocw_table_cell(), 1)) }} = columns.column_count
)

, ocw_solution_resources as (
    select distinct
        solution_cells.courserun_readable_id
        , concat({{ element_at_array('solution_cells.hrefs', 'links.link_index') }}, '/') as content_file_key
    from ocw_solution_cells as solution_cells
    cross join {{ unnest_sequence(array_length('solution_cells.hrefs'), 'links', 'link_index') }}
    where {{ array_length('solution_cells.hrefs') }} > 0

    union

    select
        courserun_readable_id
        , content_file_key
    from {{ ref('int__courseware__ocw_files') }}
    where content_type != 'page' and is_solution
)

-- OCW publishes one file under several resources; a copy of a key is a key
, ocw_solution_texts as (
    select distinct
        files.courserun_readable_id
        , {{ dbt.hash('files.body') }} as text_hash
    from {{ ref('int__courseware__ocw_files') }} as files
    inner join ocw_solution_resources as solution_resources
        on
            files.courserun_readable_id = solution_resources.courserun_readable_id
            and files.content_file_key = solution_resources.content_file_key
    -- a scan with no text layer reads as its file name, which a problem set can share
    where length(files.body) > 200
)

select
    files.courserun_readable_id
    , files.content_file_key
    , files.content_type
    , files.file_extension
    , files.title
    , files.body as resource_text
    , case
        when
            files.content_type = 'video'
            or files.file_extension in ('{{ ocw_transcript_extensions | join("', '") }}')
            or lower(files.title) like '%transcript'
            then 'video_transcript'
        else 'document'
    end as source_kind
from {{ ref('int__courseware__ocw_files') }} as files
left join ocw_solution_resources as solution_resources
    on
        files.courserun_readable_id = solution_resources.courserun_readable_id
        and files.content_file_key = solution_resources.content_file_key
left join ocw_solution_texts as solution_texts
    on
        files.courserun_readable_id = solution_texts.courserun_readable_id
        and {{ dbt.hash('files.body') }} = solution_texts.text_hash
where
    files.content_type != 'page'
    and files.body is not null
    and solution_resources.content_file_key is null
    and solution_texts.text_hash is null
