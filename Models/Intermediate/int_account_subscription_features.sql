-- models/intermediate/int_account_subscription_features.sql

{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

subscriptions as (
    select * from {{ ref('stg_subscriptions') }}
    where status = 'Active'  -- Only active subscriptions
),

subscription_summary as (
    select
        account_id,
        max(plan_type) as plan_type,  -- Assume one active plan per account
        max(monthly_cost) as monthly_cost,
        max(start_date) as subscription_start_date,
        bool_or(has_webinar_addon) as has_webinar_addon,
        bool_or(has_large_meeting_addon) as has_large_meeting_addon,
        bool_or(has_cloud_recording_addon) as has_cloud_recording_addon,
        
        -- Count total add-ons
        (case when bool_or(has_webinar_addon) then 1 else 0 end +
         case when bool_or(has_large_meeting_addon) then 1 else 0 end +
         case when bool_or(has_cloud_recording_addon) then 1 else 0 end) as total_addons
    from subscriptions
    group by account_id
)

select
    a.account_id,
    a.account_name,
    a.industry,
    a.account_size as licensed_seats,
    
    coalesce(s.plan_type, 'No Active Plan') as plan_type,
    s.monthly_cost,
    s.subscription_start_date,
    
    -- Add-on flags
    coalesce(s.has_webinar_addon, false) as has_webinar_addon,
    coalesce(s.has_large_meeting_addon, false) as has_large_meeting_addon,
    coalesce(s.has_cloud_recording_addon, false) as has_cloud_recording_addon,
    coalesce(s.total_addons, 0) as total_addons,
    
    -- Flag accounts with premium features (Enterprise or has add-ons)
    case
        when s.plan_type = 'Enterprise' or s.total_addons > 0 then true
        else false
    end as has_premium_features

from accounts a
left join subscription_summary s on a.account_id = s.account_id