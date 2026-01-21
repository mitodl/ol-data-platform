with chatsession as (
    select * from {{ ref('stg__learn_ai__app__postgres__chatbots_userchatsession') }}
)

, tutorbotoutput as (
    select * from {{ ref("stg__learn_ai__app__postgres__chatbots_tutorbotoutput") }}
)

, users as (
    select * from {{ ref('stg__learn_ai__app__postgres__users_user') }}
)

, problem as (
    select * from {{ ref('dim_problem') }}
)

select
    tutorbotoutput.tutorbotoutput_id
    , tutorbotoutput.chatsession_thread_id
    , tutorbotoutput.turorbot_chat_json
    , chatsession.chatsession_id
    , chatsession.chatsession_agent
    , chatsession.chatsession_title
    , chatsession.chatsession_object_id as edx_module_id
    , chatsession.user_id
    , problem.courserun_readable_id
    , users.user_email
    , users.user_full_name
    , users.user_username
    , users.user_global_id
    , chatsession.chatsession_django_session_key
    , chatsession.chatsession_created_on
    , chatsession.chatsession_updated_on
from tutorbotoutput
inner join chatsession on tutorbotoutput.chatsession_thread_id = chatsession.chatsession_thread_id
left join users on chatsession.user_id = users.user_id
left join problem on chatsession.chatsession_object_id = problem.problem_block_pk
