{{ config(error_if='>0') }}

-- wrike_course_code against run codes of each shape seen in the Emeritus and Global Alumni
-- feeds, plus codes it must reject. Returns the cases whose result is wrong.
with cases as (
    select *
    from (
        values
        ('MO-AIP-21-06#1', 'MO-AIP', 'AIP')
        , ('MO-DBIP.ELE-25-02#1', 'MO-DBIP.ELE', 'DBIP.ELE')
        , ('MO-DTSC.ES.ELE-24-04#1', 'MO-DTSC.ES.ELE', 'DTSC.ES.ELE')
        , ('MO-DILS.LITE-25-09#3', 'MO-DILS.LITE', 'DILS.LITE')
        , ('MO-3AI-26-09#1', 'MO-3AI', '3AI')
        , ('MXP-CRT.ES-24-12#1', 'MXP-CRT.ES', 'CRT.ES')
        , ('MO-AIP-21-06', null, null)
        , ('course-v1:xPRO+AIPSx+R1', null, null)
        , ('', null, null)
        , (null, null, null)
    ) as cases (run_code, expected_with_partner, expected_without_partner)
)

, results as (
    select
        run_code
        , expected_with_partner
        , expected_without_partner
        , {{ wrike_course_code('run_code') }} as with_partner
        , {{ wrike_course_code('run_code', include_partner=false) }} as without_partner
    from cases
)

select *
from results
where
    coalesce(with_partner, '<null>') != coalesce(expected_with_partner, '<null>')
    or coalesce(without_partner, '<null>') != coalesce(expected_without_partner, '<null>')
