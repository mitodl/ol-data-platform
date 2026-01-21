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
        , json_query(transcripts, 'lax $.en' omit quotes) as transcript_id
    from {{ ref('dim_video') }}
)

select
    djangocheckpoint.checkpoint_id
    , djangocheckpoint.chatsession_thread_id
    , chatsession.chatsession_id
    , chatsession.chatsession_agent
    , chatsession.chatsession_title
    , chatsession.chatsession_object_id
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
    , chatsession.chatsession_django_session_key
    , chatsession.chatsession_created_on
    , chatsession.chatsession_updated_on
    , video.courserun_readable_id
from djangocheckpoint
inner join chatsession on djangocheckpoint.chatsession_thread_id = chatsession.chatsession_thread_id
left join users on chatsession.user_id = users.user_id
left join video on chatsession.chatsession_object_id like '%' || video.transcript_id
