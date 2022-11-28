with flexiblepriceapplication as (
    select * from {{ ref('stg__mitxonline__app__postgres__flexiblepricing_flexiblepriceapplication') }}
)

, contenttypes as (
    select *
    from {{ ref('stg__mitxonline__app__postgres__django_contenttype') }}
)

select
    flexiblepriceapplication.flexiblepriceapplication_id
    , flexiblepriceapplication.flexiblepriceapplication_status
    , flexiblepriceapplication.flexiblepricetier_id
    , flexiblepriceapplication.user_id
    , flexiblepriceapplication.flexiblepriceapplication_created_on
    , flexiblepriceapplication.flexiblepriceapplication_income_usd
    , flexiblepriceapplication.flexiblepriceapplication_updated_on
    , flexiblepriceapplication.flexiblepriceapplication_justification
    , flexiblepriceapplication.flexiblepriceapplication_original_income
    , flexiblepriceapplication.flexiblepriceapplication_country_of_income
    , flexiblepriceapplication.flexiblepriceapplication_original_currency
    , flexiblepriceapplication.flexiblepriceapplication_exchange_rate_timestamp
    , flexiblepriceapplication.flexiblepriceapplication_date_documents_sent
    , flexiblepriceapplication.flexiblepriceapplication_country_of_residence

    , case contenttypes.contenttype_full_name
        when 'courses_course' then flexiblepriceapplication.courseware_object_id
    end as course_id

    , case contenttypes.contenttype_full_name
        when 'courses_program' then flexiblepriceapplication.courseware_object_id
    end as program_id

    , case contenttypes.contenttype_full_name
        when 'courses_course' then 'course'
        when 'courses_program' then 'program'
    end as courseware_type
from flexiblepriceapplication
inner join contenttypes on flexiblepriceapplication.contenttype_id = contenttypes.contenttype_id
