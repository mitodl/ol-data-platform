--MITxPro Online Course Information

with source as (
    select * from {{ source('ol_warehouse_raw_data','raw__xpro__app__postgres__courses_course') }}
)

, cleaned as (
    select
        id as course_id
        , live as course_is_live
        , title as course_title
        , program_id
        , readable_id as course_readable_id
        , case 
            when 
                substring(
                    (replace(replace(readable_id, 'course-v1:', ''), '+', '/'))
                    , (position('/' in (replace(replace(readable_id, 'course-v1:', ''), '+', '/'))) + 1)
                ) like '%x%'
                then 
                    substring(
                        (replace(replace(readable_id, 'course-v1:', ''), '+', '/'))
                        , (position('/' in (replace(replace(readable_id, 'course-v1:', ''), '+', '/'))) + 1)
                        , position(
                            'x' in (
                                substring(
                                    replace(replace(readable_id, 'course-v1:', ''), '+', '/')
                                    , (position('/' in eplace(replace(readable_id, 'course-v1:', ''), '+', '/')) + 1)
                                )
                            )
                        ) - 1
                    )
            when 
                substring(
                    replace(replace(readable_id, 'course-v1:', ''), '+', '/')
                    , (position('/' in replace(replace(readable_id, 'course-v1:', ''), '+', '/')) + 1)
                ) like '%/%'
                then 
                    substring(
                        replace(replace(readable_id, 'course-v1:', ''), '+', '/')
                        , (position('/' in replace(replace(readable_id, 'course-v1:', ''), '+', '/')) + 1)
                        , (
                            position(
                                '/' in (
                                    substring(
                                        replace(replace(readable_id, 'course-v1:', ''), '+', '/')
                                        , (
                                            position(
                                                '/' in 
                                                replace(replace(readable_id, 'course-v1:', ''), '+', '/')
                                            ) + 1
                                        )
                                    )
                                )
                            ) - 1
                        )
                    )
            else 
                substring(
                    replace(replace(readable_id, 'course-v1:', ''), '+', '/')
                    , (position('/' in replace(replace(readable_id, 'course-v1:', ''), '+', '/')) + 1)
                ) 
        end as program_code
        , position_in_program
        , platform_id
        , replace(replace(readable_id, 'course-v1:', ''), '+', '/') as course_edx_readable_id
        ,{{ cast_timestamp_to_iso8601('created_on') }} as course_created_on
        ,{{ cast_timestamp_to_iso8601('updated_on') }} as course_updated_on
        , is_external as course_is_external
    from source
)

select * from cleaned
