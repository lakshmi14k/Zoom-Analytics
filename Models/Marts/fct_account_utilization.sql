-- models/marts/fct_account_utilization.sql
-- Final fact table: Account utilization scorecard for expansion opportunities

{{
    config(
        materialized='table',
        schema='marts'
    )
}}

with user_summary as (
    select * from {{ ref('int_account_user_summary') }}
),

subscription_features as (
    select * from {{ ref('int_account_subscription_features') }}
),

event_usage as (
    select * from {{ ref('int_account_event_usage') }}
)

select
    -- Account identifiers
    u.account_id,
    u.account_name,
    u.industry,
    u.account_created_at,
    
    -- Seat utilization metrics
    u.licensed_seats,
    u.total_users,
    u.active_users,
    u.ghost_users,
    u.seat_utilization_pct,
    u.is_underutilized_seats,
    u.most_recent_user_activity,
    
    -- Subscription & revenue
    s.plan_type,
    s.monthly_cost,
    s.subscription_start_date,
    s.has_webinar_addon,
    s.has_large_meeting_addon,
    s.has_cloud_recording_addon,
    s.total_addons,
    s.has_premium_features,
    
    -- Event usage metrics
    e.total_events,
    e.unique_hosts,
    e.meeting_count,
    e.webinar_count,
    e.total_duration_mins,
    e.avg_duration_mins,
    e.avg_participants_per_event,
    
    -- Feature usage percentages
    e.screen_share_usage_pct,
    e.recording_usage_pct,
    e.breakout_rooms_usage_pct,
    e.basic_meeting_pct,
    e.is_basic_usage_only,
    e.first_event_date,
    e.last_event_date,
    
    -- CALCULATED SCORES (THE KEY METRICS!)
    
    -- Feature Adoption Score (0-100)
    -- Measures how much of available features they're actually using
    round(
        case
            when s.has_premium_features then
                ((case when e.screen_share_usage_pct > 0 then 25 else 0 end) +
                 (case when e.recording_usage_pct > 0 then 25 else 0 end) +
                 (case when e.breakout_rooms_usage_pct > 0 then 25 else 0 end) +
                 (case when e.webinar_count > 0 then 25 else 0 end))
            else null  -- Only calculate for premium accounts
        end, 2
    ) as feature_adoption_score,
    
    -- Underutilization Severity Score (0-100, higher = worse)
    -- Combines seat waste + feature waste + engagement
    round(
        (
            -- Seat underutilization (40% weight)
            (case when u.seat_utilization_pct < 50 then 40 else 0 end) +
            
            -- Basic usage only (30% weight)
            (case when e.is_basic_usage_only then 30 else 0 end) +
            
            -- Has paid add-ons but not using them (30% weight)
            (case 
                when s.has_webinar_addon and e.webinar_count = 0 then 10
                when s.has_large_meeting_addon and e.avg_participants_per_event < 50 then 10
                when s.has_cloud_recording_addon and e.recording_usage_pct < 10 then 10
                else 0
            end)
        ), 2
    ) as underutilization_severity_score,
    
    -- Revenue at Risk (monthly cost they might churn on)
    case
        when u.seat_utilization_pct < 25 or e.is_basic_usage_only then s.monthly_cost
        when u.seat_utilization_pct < 50 then s.monthly_cost * 0.5
        else 0
    end as revenue_at_risk,
    
    -- Expansion Opportunity Score (0-100, higher = better opportunity)
    round(
        case
            when s.plan_type in ('Enterprise', 'Pro') and s.total_addons > 0 then
                (
                    -- Large account (20 points)
                    (case when u.licensed_seats > 200 then 20 else u.licensed_seats / 10 end) +
                    
                    -- High event volume (20 points)
                    (case when e.total_events > 100 then 20 else e.total_events / 5 end) +
                    
                    -- Low feature adoption = training opportunity (30 points)
                    (case 
                        when e.basic_meeting_pct > 80 then 30
                        when e.basic_meeting_pct > 50 then 20
                        else 10
                    end) +
                    
                    -- Underutilized seats = consolidation opportunity (30 points)
                    (case
                        when u.seat_utilization_pct < 25 then 30
                        when u.seat_utilization_pct < 50 then 20
                        else 10
                    end)
                )
            else 0
        end, 2
    ) as expansion_opportunity_score,
    
    -- Engagement health flags
    case
        when e.total_events = 0 then 'Inactive'
        when e.total_events < 10 then 'Low'
        when e.total_events < 50 then 'Medium'
        else 'High'
    end as engagement_level,
    
    -- Account health status
    case
        when u.seat_utilization_pct < 25 and e.is_basic_usage_only then 'Critical'
        when u.is_underutilized_seats or e.basic_meeting_pct > 80 then 'At Risk'
        when u.seat_utilization_pct > 70 and not e.is_basic_usage_only then 'Healthy'
        else 'Moderate'
    end as account_health_status,
    
    -- Primary expansion action recommendation
    case
        when e.total_events = 0 then 'Re-engage - No Usage'
        when u.seat_utilization_pct < 30 then 'Consolidate Seats'
        when s.has_premium_features and e.is_basic_usage_only then 'Feature Training'
        when s.plan_type = 'Basic' and e.total_events > 50 then 'Upgrade to Pro'
        when s.plan_type = 'Pro' and u.licensed_seats > 200 then 'Upgrade to Enterprise'
        else 'Monitor'
    end as recommended_action

from user_summary u
left join subscription_features s on u.account_id = s.account_id
left join event_usage e on u.account_id = e.account_id