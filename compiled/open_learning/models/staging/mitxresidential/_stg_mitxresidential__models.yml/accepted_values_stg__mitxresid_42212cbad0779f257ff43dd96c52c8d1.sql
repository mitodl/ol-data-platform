with all_values as (

    select
        courseaccessrole_role as value_field
        , count(*) as n_records

    from dev.main_staging.stg__mitxresidential__openedx__user_courseaccessrole
    group by courseaccessrole_role

)

select *
from all_values
where value_field not in (
    'staff'
    , 'data_researcher'
    , 'instructor'
    , 'beta_testers'
    , 'finance_admin'
    , 'sales_admin'
    , 'library_user'
    , 'org_course_creator_group'
    , 'library_user'
    , 'support'
)
