with courseruns as (select * from {{ ref("stg__edxorg__api__courserun") }})

select
    {{ format_course_id("courseruns.courserun_readable_id") }} as courserun_readable_id,
    cast(json_extract_scalar(t.seat, '$.price') as decimal(38, 2)) as price,
    json_extract_scalar(t.seat, '$.type') as courserun_mode,
    json_extract_scalar(t.seat, '$.currency') as currency,
    json_extract_scalar(t.seat, '$.upgrade_deadline') as upgrade_deadline,
    json_extract_scalar(t.seat, '$.credit_provider') as credit_provider,
    json_extract_scalar(t.seat, '$.credit_hours') as credit_hours
from courseruns
cross join unnest(cast(json_parse(courseruns.courserun_enrollment_modes) as array(json))) as t(seat)  -- noqa
