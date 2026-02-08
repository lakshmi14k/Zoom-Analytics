-- models/intermediate/int_account_user_summary.sql

{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

users as (
    select * from {{ ref('stg_users') }}
),

user_summary as (
    select
        account_id,
        count(*) as total_users,
        count(case when is_ghost_user = false then 1 end) as active_users,
        count(case when is_ghost_user = true then 1 end) as ghost_users,
        max(last_active_date) as most_recent_user_activity
    from users
    group by account_id
)

select
    a.account_id,
    a.account_name,
    a.industry,
    a.created_at as account_created_at,
    a.account_size as licensed_seats,
    
    coalesce(u.total_users, 0) as total_users,
    coalesce(u.active_users, 0) as active_users,
    coalesce(u.ghost_users, 0) as ghost_users,
    u.most_recent_user_activity,
    
    -- Calculate seat utilization percentage
    case
        when a.account_size > 0 then 
            round((coalesce(u.active_users, 0)::numeric / a.account_size::numeric) * 100, 2)
        else null
    end as seat_utilization_pct,
    
    -- Flag underutilized accounts (less than 50% seat usage)
    case
        when a.account_size > 0 and 
             (coalesce(u.active_users, 0)::numeric / a.account_size::numeric) < 0.5 
        then true
        else false
    end as is_underutilized_seats

from accounts a
left join user_summary u on a.account_id = u.account_id