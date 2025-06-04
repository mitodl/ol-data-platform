with video_pre_query as (
    select
        platform
        , openedx_user_id
        , courserun_readable_id
        , video_block_fk
        , max(video_duration) as video_duration
        , max(video_position) as end_time
        , min(case when event_type = 'play_video' then video_position end) as start_time
    from ol_warehouse_production_dimensional.tfact_video_events
    where
        event_type in (
            'play_video'
            , 'seek_video'
            , 'pause_video'
            , 'stop_video'
            , 'complete_video'
        )
    group by
        platform
        , openedx_user_id
        , courserun_readable_id
        , video_block_fk
)

, discuss_table as (
    select
        a.platform
        , a.courserun_readable_id
        , d.block_title as section_title
        , c.block_title as subsection_title
        , coalesce(h.course_title, h2.course_title) as course_title
        , coalesce(u.email, ou.email) as email
        , sum(a.post_created) as posts_created
        , sum(a.post_replied) as posts_replied
    from ol_warehouse_production_dimensional.afact_discussion_engagement as a
    left join ol_warehouse_production_dimensional.dim_user as u
        on a.platform = 'mitxonline' and a.openedx_user_id = u.mitxonline_openedx_user_id
    left join ol_warehouse_production_dimensional.dim_user as ou
        on a.platform = 'edxorg' and a.openedx_user_id = ou.edxorg_openedx_user_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h
        on replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = h.courserun_readable_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h2
        on a.courserun_readable_id = h2.courserun_readable_id
    left join ol_warehouse_production_dimensional.dim_course_content as c
        on
            a.sequential_block_fk = c.block_id
            and c.is_latest = true
    left join ol_warehouse_production_dimensional.dim_course_content as d
        on
            a.chapter_block_fk = d.block_id
            and d.is_latest = true
    group by
        a.platform
        , a.courserun_readable_id
        , d.block_title
        , c.block_title
        , coalesce(h.course_title, h2.course_title)
        , coalesce(u.email, ou.email)
)

, page_views_table as (
    select
        a.platform
        , a.courserun_readable_id
        , d.block_title as section_title
        , c.block_title as subsection_title
        , coalesce(h.course_title, h2.course_title) as course_title
        , coalesce(u.email, ou.email) as email
        , sum(a.num_of_views) as num_of_page_views
    from ol_warehouse_production_dimensional.afact_course_page_engagement as a
    inner join ol_warehouse_production_dimensional.dim_course_content as b
        on
            a.block_fk = b.block_id
            and b.is_latest = true
    inner join ol_warehouse_production_dimensional.dim_course_content as c
        on
            b.parent_block_id = c.block_id
            and c.is_latest = true
            and replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = c.courserun_readable_id
    inner join ol_warehouse_production_dimensional.dim_course_content as d
        on
            c.parent_block_id = d.block_id
            and replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = d.courserun_readable_id
            and d.is_latest = true
    left join ol_warehouse_production_dimensional.dim_user as u
        on
            a.platform = 'mitxonline'
            and a.openedx_user_id = u.mitxonline_openedx_user_id
    left join ol_warehouse_production_dimensional.dim_user as ou
        on
            a.platform = 'edxorg'
            and a.openedx_user_id = ou.edxorg_openedx_user_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h
        on replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = h.courserun_readable_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h2
        on a.courserun_readable_id = h2.courserun_readable_id
    group by
        a.platform
        , a.courserun_readable_id
        , d.block_title
        , c.block_title
        , coalesce(h.course_title, h2.course_title)
        , coalesce(u.email, ou.email)
)

, video_views_table as (
    select
        a.platform
        , a.courserun_readable_id
        , cc_section.block_title as section_title
        , cc_subsection.block_title as subsection_title
        , coalesce(h.course_title, h2.course_title) as course_title
        , coalesce(b.email, ob.email) as email
        , sum(
            cast(case when a.end_time = 'null' then '0' else a.end_time end as decimal(30, 10))
            - cast(case when a.start_time = 'null' then '0' else a.start_time end as decimal(30, 10))
        )
        as estimated_time_played
        , sum(a.video_duration) as video_duration
    from video_pre_query as a
    inner join ol_warehouse_production_dimensional.dim_video as c
        on
            replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = c.courserun_readable_id
            and a.video_block_fk = substring(c.video_block_pk, regexp_position(c.video_block_pk, 'block@') + 6)
    left join ol_warehouse_production_dimensional.dim_user as b
        on
            a.platform = 'mitxonline'
            and a.openedx_user_id = b.mitxonline_openedx_user_id
    left join ol_warehouse_production_dimensional.dim_user as ob
        on
            a.platform = 'edxorg'
            and a.openedx_user_id = ob.edxorg_openedx_user_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h
        on replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = h.courserun_readable_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h2
        on a.courserun_readable_id = h2.courserun_readable_id
    inner join ol_warehouse_production_dimensional.dim_course_content as v
        on
            c.content_block_fk = v.content_block_pk
            and replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = v.courserun_readable_id
    inner join ol_warehouse_production_dimensional.dim_course_content as cc_subsection
        on
            v.parent_block_id = cc_subsection.block_id
            and replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/')
            = cc_subsection.courserun_readable_id
            and cc_subsection.is_latest = true
    inner join ol_warehouse_production_dimensional.dim_course_content as cc_section
        on
            cc_subsection.parent_block_id = cc_section.block_id
            and replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = cc_section.courserun_readable_id
            and cc_section.is_latest = true
    group by
        a.platform
        , a.courserun_readable_id
        , cc_section.block_title
        , cc_subsection.block_title
        , coalesce(h.course_title, h2.course_title)
        , coalesce(b.email, ob.email)
)

, pre_problems_table as (
    select
        c.sequential_block_id
        , count(p.problem_block_pk) as problem_numb
    from ol_warehouse_production_dimensional.dim_problem as p
    inner join ol_warehouse_production_dimensional.dim_course_content as c
        on p.content_block_fk = c.content_block_pk
    group by c.sequential_block_id
)

, problems_events as (
    select
        problem_block_fk
        , courserun_readable_id
        , openedx_user_id
        , avg(
            case when max_grade = '0' then
                0 else (cast(grade as decimal(30, 10))
            / cast(max_grade as decimal(30, 10))) end
        )
        as avg_percent_grade
    from ol_warehouse_production_dimensional.tfact_problem_events
    group by
        problem_block_fk
        , courserun_readable_id
        , openedx_user_id
)

, problems_table as (
    select
        a.platform
        , a.courserun_readable_id
        , sec.block_title as section_title
        , d.block_title as subsection_title
        , coalesce(h.course_title, h2.course_title) as course_title
        , coalesce(u.email, ou.email) as email
        , avg(g.avg_percent_grade) as avg_percent_grade
        , count(distinct case when cast(a.num_of_attempts as int) > 0 then a.problem_block_fk end) as problems_attempted
        , max(c.problem_numb) as number_of_problems
        , cast(count(distinct case
            when cast(a.num_of_attempts as int) > 0
                then a.problem_block_fk
        end) as decimal(30, 10))
        / cast(max(c.problem_numb) as decimal(30, 10)
        ) as percetage_problems_attempted
    from ol_warehouse_production_dimensional.afact_problem_engagement as a
    inner join pre_problems_table as c
        on a.sequential_block_fk = c.sequential_block_id
    left join ol_warehouse_production_dimensional.dim_user as u
        on
            a.platform = 'mitxonline'
            and a.openedx_user_id = u.mitxonline_openedx_user_id
    left join ol_warehouse_production_dimensional.dim_user as ou
        on
            a.platform = 'edxorg'
            and a.openedx_user_id = ou.edxorg_openedx_user_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h
        on replace(replace(a.courserun_readable_id, 'course-v1:', ''), '+', '/') = h.courserun_readable_id
    left join ol_warehouse_production_intermediate.int__combined__course_runs as h2
        on a.courserun_readable_id = h2.courserun_readable_id
    inner join ol_warehouse_production_dimensional.dim_course_content as d
        on
            a.sequential_block_fk = d.block_id
            and d.is_latest = true
    inner join ol_warehouse_production_dimensional.dim_course_content as sec
        on
            a.chapter_block_fk = sec.block_id
            and sec.is_latest = true
    inner join problems_events as g
        on
            a.problem_block_fk = g.problem_block_fk
            and a.courserun_readable_id = g.courserun_readable_id
            and a.openedx_user_id = g.openedx_user_id
    group by
        a.platform
        , a.courserun_readable_id
        , sec.block_title
        , d.block_title
        , coalesce(h.course_title, h2.course_title)
        , coalesce(u.email, ou.email)
)

, page_and_video as (
    select
        page_views_table.num_of_page_views
        , video_views_table.estimated_time_played
        , video_views_table.video_duration
        , coalesce(video_views_table.platform, page_views_table.platform) as platform
        , coalesce(video_views_table.email, page_views_table.email) as email
        , coalesce(video_views_table.course_title, page_views_table.course_title) as course_title
        , coalesce(video_views_table.courserun_readable_id, page_views_table.courserun_readable_id)
        as courserun_readable_id
        , coalesce(video_views_table.section_title, page_views_table.section_title) as section_title
        , coalesce(video_views_table.subsection_title, page_views_table.subsection_title) as subsection_title
    from page_views_table
    full outer join video_views_table
        on
            page_views_table.email = video_views_table.email
            and page_views_table.courserun_readable_id = video_views_table.courserun_readable_id
            and page_views_table.section_title = video_views_table.section_title
            and page_views_table.subsection_title = video_views_table.subsection_title
)

, page_video_problems as (
    select
        page_and_video.num_of_page_views
        , page_and_video.estimated_time_played
        , page_and_video.video_duration
        , problems_table.problems_attempted
        , problems_table.number_of_problems
        , problems_table.percetage_problems_attempted
        , problems_table.avg_percent_grade
        , coalesce(page_and_video.platform, problems_table.platform) as platform
        , coalesce(page_and_video.email, problems_table.email) as email
        , coalesce(page_and_video.course_title, problems_table.course_title) as course_title
        , coalesce(page_and_video.courserun_readable_id, problems_table.courserun_readable_id) as courserun_readable_id
        , coalesce(page_and_video.section_title, problems_table.section_title) as section_title
        , coalesce(page_and_video.subsection_title, problems_table.subsection_title) as subsection_title
    from page_and_video
    full outer join problems_table
        on
            page_and_video.email = problems_table.email
            and page_and_video.courserun_readable_id = problems_table.courserun_readable_id
            and page_and_video.section_title = problems_table.section_title
            and page_and_video.subsection_title = problems_table.subsection_title
)

select
    page_video_problems.num_of_page_views
    , page_video_problems.estimated_time_played
    , page_video_problems.video_duration
    , page_video_problems.problems_attempted
    , page_video_problems.number_of_problems
    , page_video_problems.percetage_problems_attempted
    , page_video_problems.avg_percent_grade
    , discuss_table.posts_created
    , discuss_table.posts_replied
    , coalesce(page_video_problems.platform, discuss_table.platform) as platform
    , coalesce(page_video_problems.email, discuss_table.email) as user_email
    , coalesce(page_video_problems.course_title, discuss_table.course_title) as course_title
    , coalesce(page_video_problems.courserun_readable_id, discuss_table.courserun_readable_id) as courserun_readable_id
    , coalesce(page_video_problems.section_title, discuss_table.section_title) as section_title
    , coalesce(page_video_problems.subsection_title, discuss_table.subsection_title) as subsection_title
from page_video_problems
full outer join discuss_table
    on
        page_video_problems.email = discuss_table.email
        and page_video_problems.courserun_readable_id = discuss_table.courserun_readable_id
        and page_video_problems.section_title = discuss_table.section_title
        and page_video_problems.subsection_title = discuss_table.subsection_title
