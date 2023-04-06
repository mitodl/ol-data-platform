with child as (
    select coursetopic_id as from_field
    from dev.main_staging.stg__mitxonline__app__postgres__courses_course_to_topic
    where coursetopic_id is not null
)

, parent as (
    select coursetopic_id as to_field
    from dev.main_staging.stg__mitxonline__app__postgres__courses_coursetopic
)

select from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null
