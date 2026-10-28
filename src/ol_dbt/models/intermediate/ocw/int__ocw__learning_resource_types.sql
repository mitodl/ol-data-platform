select distinct
    resources.course_uuid
    , resources.resource_uuid
    , resources.learning_resource_type
from {{ ref('int__ocw__resources') }} as resources
cross join unnest(split(resources.learning_resource_types, ', ')) as types (learning_resource_type)
where resources.learning_resource_type <> ''
