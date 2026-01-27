-- models/intermediate/int_account_event_usage.sql

{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

events as (
    select * from {{ ref('stg_events') }}
    where event_date is not null
),

event_summary as (
    select
        account_id,
        
        -- Event counts
        count(*) as total_events,
        count(distinct user_id) as unique_hosts,
        count(case when event_type = 'Meeting' then 1 end) as meeting_count,
        count(case when event_type = 'Webinar' then 1 end) as webinar_count,
        
        -- Usage metrics
        sum(duration_mins) as total_duration_mins,
        avg(duration_mins) as avg_duration_mins,
        sum(participants_count) as total_participants,
        avg(participants_count) as avg_participants_per_event,
        
        -- Feature usage counts (KEY for underutilization analysis!)
        count(case when used_screen_share then 1 end) as screen_share_usage_count,
        count(case when used_recording then 1 end) as recording_usage_count,
        count(case when used_breakout_rooms then 1 end) as breakout_rooms_usage_count,
        count(case when used_polling then 1 end) as polling_usage_count,
        
        -- Basic meeting flag (no advanced features)
        count(case when is_basic_meeting then 1 end) as basic_meeting_count,
        
        -- Calculate feature usage percentages
        round((count(case when used_screen_share then 1 end)::numeric / count(*)::numeric) * 100, 2) as screen_share_usage_pct,
        round((count(case when used_recording then 1 end)::numeric / count(*)::numeric) * 100, 2) as recording_usage_pct,
        round((count(case when used_breakout_rooms then 1 end)::numeric / count(*)::numeric) * 100, 2) as breakout_rooms_usage_pct,
        round((count(case when is_basic_meeting then 1 end)::numeric / count(*)::numeric) * 100, 2) as basic_meeting_pct,
        
        -- Date range
        min(event_date) as first_event_date,
        max(event_date) as last_event_date
        
    from events
    group by account_id
)

select
    a.account_id,
    a.account_name,
    a.industry,
    a.account_size as licensed_seats,
    
    -- Event metrics
    coalesce(e.total_events, 0) as total_events,
    coalesce(e.unique_hosts, 0) as unique_hosts,
    coalesce(e.meeting_count, 0) as meeting_count,
    coalesce(e.webinar_count, 0) as webinar_count,
    
    -- Usage metrics
    coalesce(e.total_duration_mins, 0) as total_duration_mins,
    round(coalesce(e.avg_duration_mins, 0), 2) as avg_duration_mins,
    coalesce(e.total_participants, 0) as total_participants,
    round(coalesce(e.avg_participants_per_event, 0), 2) as avg_participants_per_event,
    
    -- Feature usage counts
    coalesce(e.screen_share_usage_count, 0) as screen_share_usage_count,
    coalesce(e.recording_usage_count, 0) as recording_usage_count,
    coalesce(e.breakout_rooms_usage_count, 0) as breakout_rooms_usage_count,
    coalesce(e.polling_usage_count, 0) as polling_usage_count,
    coalesce(e.basic_meeting_count, 0) as basic_meeting_count,
    
    -- Feature usage percentages
    coalesce(e.screen_share_usage_pct, 0) as screen_share_usage_pct,
    coalesce(e.recording_usage_pct, 0) as recording_usage_pct,
    coalesce(e.breakout_rooms_usage_pct, 0) as breakout_rooms_usage_pct,
    coalesce(e.basic_meeting_pct, 0) as basic_meeting_pct,
    
    -- Date range
    e.first_event_date,
    e.last_event_date,
    
    -- Flag accounts with NO advanced feature usage
    case
        when coalesce(e.basic_meeting_pct, 0) >= 90 then true
        else false
    end as is_basic_usage_only

from accounts a
left join event_summary e on a.account_id = e.account_id

