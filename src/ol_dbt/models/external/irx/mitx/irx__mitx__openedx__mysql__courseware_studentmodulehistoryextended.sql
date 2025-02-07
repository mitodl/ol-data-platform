with studentmodulehistoryextended as (
    select * from {{ ref('stg__mitxresidential__openedx__courseware_studentmodulehistoryextended') }}
)

, studentmodule as (
    select * from {{ ref('stg__mitxresidential__openedx__courseware_studentmodule') }}
)

select
    studentmodule.courserun_readable_id as course_id
    , studentmodule.user_id
    , studentmodule.coursestructure_block_id as module_id
    , studentmodule.coursestructure_block_category as module_type
    , studentmodulehistoryextended.studentmodule_state_data as state_data
    , studentmodulehistoryextended.studentmodule_problem_grade as grade
    , studentmodulehistoryextended.studentmodule_problem_max_grade as max_grade
    , studentmodulehistoryextended.studentmodule_created_on as created
from studentmodulehistoryextended
inner join studentmodule on studentmodulehistoryextended.studentmodule_id = studentmodule.studentmodule_id
