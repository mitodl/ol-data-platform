with
    source as (select * from {{ source("ol_warehouse_raw_data", "raw__ocw__studio__postgres__gdrive_sync_drivefile") }})

select
    file_id as drivefile_id, name as drivefile_name, mime_type as drivefile_mime_type, resource_id as websitecontent_id
from source
