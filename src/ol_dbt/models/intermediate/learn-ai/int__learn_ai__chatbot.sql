with chatsession as (
    select * from {{ ref('stg__learn_ai__app__postgres__chatbots_userchatsession') }}
)

, djangocheckpoint as (
    select * from {{ ref('stg__learn_ai__app__postgres__chatbots_djangocheckpoint') }}
)

, users as (
    select * from {{ ref('stg__learn_ai__app__postgres__users_user') }}
)

, video as (
    select distinct
        courserun_readable_id
        , retrieved_at
        , json_query(block_metadata, 'lax $.transcripts.en' omit quotes) as transcript_id
    from {{ ref('dim_course_content') }}
    where block_category = 'video'
)

, videos_with_ranking as (
    select
        video.courserun_readable_id
        , chatsession.chatsession_object_id
        , row_number() over (
            partition by chatsession.chatsession_object_id
            order by video.retrieved_at asc
        ) as row_num
    from chatsession
    inner join video
        on
            chatsession.chatsession_object_id like '%' || video.transcript_id
            and video.courserun_readable_id like '%'
            || substring(
                replace(chatsession.chatsession_object_id, 'asset-v1:', ''), 1
                , strpos(replace(chatsession.chatsession_object_id, 'asset-v1:', ''), '+type@asset+block@') - 1
            )
            and chatsession.chatsession_created_on < video.retrieved_at
)

select
    djangocheckpoint.checkpoint_id
    , djangocheckpoint.chatsession_thread_id
    , chatsession.chatsession_agent
    , chatsession.chatsession_title
    , chatsession.chatsession_object_id
    , videos_with_ranking.courserun_readable_id
    , chatsession.user_id
    , users.user_email
    , users.user_full_name
    , users.user_username
    , users.user_global_id
    , djangocheckpoint.checkpoint_json
    , djangocheckpoint.checkpoint_metadata
    , djangocheckpoint.parent_checkpoint_id
    , djangocheckpoint.checkpoint_namespace
    , djangocheckpoint.checkpoint_type
    , chatsession.chatsession_created_on
    , chatsession.chatsession_updated_on
from djangocheckpoint
inner join chatsession on djangocheckpoint.chatsession_thread_id = chatsession.chatsession_thread_id
left join users on chatsession.user_id = users.user_id
left join videos_with_ranking
    on
        chatsession.chatsession_object_id = videos_with_ranking.chatsession_object_id
        and videos_with_ranking.row_num = 1
