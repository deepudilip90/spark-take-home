-- how many total messages are sent each day
select 
    date(created_at) as calendar_date, 
    count(*) as total_messages 
from spark_dwh.messages_raw
group by calendar_date;

-- users that did not receive any messages
select 
    a.* 
from
spark_dwh.users_raw a 
left join spark_dwh.messages_raw b 
     on a.user_id = b.receiver_id 
where b.receiver_id is null;

-- active subscriptions as of today
select * from spark_dwh.subscriptions_raw
where lower(status) = 'active' and date(end_date) >= current_date();

-- users sending messages without active subscriptions
with fulfilled_subscriptions as (
select * from spark_dwh.subscriptions_raw 
where status <> 'Rejected'),

messages_with_subscription_status as (
select a.*,
case when b.user_id is not null then 1 else 0 end as was_subscription_active
from spark_dwh.messages_raw a
left join 
fulfilled_subscriptions b
on a.sender_id = b.user_id and a.created_at between b.start_date and b.end_date
)
select * from messages_with_subscription_status where was_subscription_active = 0;