{{ config(materialized='view') }}

with mitxonline_studentmodule_problems as
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxonline__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxonline__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id'
    ) }}

, mitxpro_studentmodule_problems as
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxpro__openedx__mysql__courseware_studentmodule'),
        ref('stg__mitxpro__openedx__courseware_studentmodulehistoryextended'),
        'openedx_user_id'
    ) }}

, mitxresidential_studentmodule_problems as
    {{ generate_studentmodule_problem_events(
        ref('stg__mitxresidential__openedx__courseware_studentmodule'),
        ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended'),
        'user_id'
    ) }}

, edxorg_studentmodule_problems as
    {{ generate_studentmodule_problem_events(
        ref('stg__edxorg__s3__courseware_studentmodule'),
        'null',
        'user_id'
    ) }}

select
    *
    , 'mitxonline' as platform
from mitxonline_studentmodule_problems

union all

select
    *
    , 'mitxpro' as platform
from mitxpro_studentmodule_problems

union all

select
    *
    , 'residential' as platform
from mitxresidential_studentmodule_problems

union all

select
    *
    , 'edxorg' as platform
from edxorg_studentmodule_problems
