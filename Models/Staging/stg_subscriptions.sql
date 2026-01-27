-- models/staging/stg_subscriptions.sql

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'subscriptions') }}
),

cleaned as (
    select
        -- Clean subscription_id
        trim(subscription_id) as subscription_id,
        
        -- Clean account_id
        trim(account_id) as account_id,
        
        -- Standardize plan_type
        case
            when trim(lower(plan_type)) in ('', 'null', 'n/a') then null
            when trim(lower(plan_type)) in ('basic', 'Basic', 'BASIC') then 'Basic'
            when trim(lower(plan_type)) in ('pro', 'Pro', 'PRO') then 'Pro'
            when trim(lower(plan_type)) in ('enterprise', 'Enterprise', 'ENTERPRISE', 'enterprize') then 'Enterprise'
            when trim(lower(plan_type)) = 'business' then 'Pro'
            else initcap(trim(plan_type))
        end as plan_type,
        
        -- Clean and parse add_ons
        case
            when trim(add_ons) in ('', 'null', 'n/a', 'NULL') then null
            when add_ons like '%;%' then replace(lower(trim(add_ons)), ';', ',')
            when add_ons like '%|%' then replace(replace(lower(trim(add_ons)), ' | ', ','), '|', ',')
            else lower(trim(add_ons))
        end as add_ons,
        
        -- Parse individual add-on flags
        case
            when lower(add_ons) like '%webinar%' then true
            else false
        end as has_webinar_addon,
        
        case
            when lower(add_ons) like '%large meeting%' or lower(add_ons) like '%large_meeting%' then true
            else false
        end as has_large_meeting_addon,
        
        case
            when lower(add_ons) like '%cloud recording%' or lower(add_ons) like '%recording%' then true
            else false
        end as has_cloud_recording_addon,
        
        -- Parse start_date
        case
            when trim(start_date) in ('', 'N/A', 'NULL', 'Unknown') then null
            when start_date ~ E'^\\d{4}-\\d{2}-\\d{2}$' then start_date::date
            when start_date ~ E'^\\d{2}/\\d{2}/\\d{4}$' then to_date(start_date, 'MM/DD/YYYY')
            when start_date ~ E'^\\d{2}-\\d{2}-\\d{4}$' then to_date(start_date, 'DD-MM-YYYY')
            when start_date ~ E'^\\d{4}/\\d{2}/\\d{2}$' then to_date(replace(start_date, '/', '-'), 'YYYY-MM-DD')
            else null
        end as start_date,
        
        -- Clean monthly_cost
        case
            when trim(monthly_cost::text) in ('', 'N/A', 'NULL', 'null') then null
            when monthly_cost::text ~ E'^-?\\d+\\.?\\d*$' then 
                case
                    when monthly_cost::numeric <= 0 then null
                    when monthly_cost::numeric > 10000 then null
                    else monthly_cost::numeric
                end
            else null
        end as monthly_cost,
        
        -- Standardize status
        case
            when trim(lower(status)) in ('', 'null', 'n/a') then null
            when trim(lower(status)) in ('active', 'Active', 'ACTIVE') then 'Active'
            when trim(lower(status)) in ('cancelled', 'Cancelled', 'CANCELLED') then 'Cancelled'
            when trim(lower(status)) in ('trial', 'Trial', 'TRIAL') then 'Trial'
            else initcap(trim(status))
        end as status,
        
        current_timestamp as dbt_loaded_at

    from source
    where 
        trim(subscription_id) not in ('', 'NULL', 'N/A')
        and subscription_id is not null
)

select distinct * from cleaned