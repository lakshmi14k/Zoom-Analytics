-- models/staging/stg_events.sql

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'events') }}
),

cleaned as (
    select
        -- Clean event_id
        trim(event_id) as event_id,
        
        -- Clean account_id
        trim(account_id) as account_id,
        
        -- Clean user_id
        trim(user_id) as user_id,
        
        -- Standardize event_type
        case
            when trim(lower(event_type)) in ('', 'null', 'n/a') then null
            when trim(lower(event_type)) in ('meeting', 'Meeting', 'MEETING') then 'Meeting'
            when trim(lower(event_type)) in ('webinar', 'Webinar', 'WEBINAR') then 'Webinar'
            when trim(lower(event_type)) in ('phone', 'Phone', 'PHONE') then 'Phone'
            else initcap(trim(event_type))
        end as event_type,
        
        -- Parse event_date
        case
            when trim(event_date) in ('', 'N/A', 'NULL', 'Unknown') then null
            when event_date ~ E'^\\d{4}-\\d{2}-\\d{2}$' then event_date::date
            when event_date ~ E'^\\d{2}/\\d{2}/\\d{4}$' then to_date(event_date, 'MM/DD/YYYY')
            when event_date ~ E'^\\d{2}-\\d{2}-\\d{4}$' then to_date(event_date, 'DD-MM-YYYY')
            when event_date ~ E'^\\d{4}/\\d{2}/\\d{2}$' then to_date(replace(event_date, '/', '-'), 'YYYY-MM-DD')
            else null
        end as event_date,
        
        -- Clean duration_mins
        case
            when trim(duration_mins::text) in ('', 'N/A', 'NULL', 'null') then null
            when duration_mins::text ~ E'^-?\\d+\\.?\\d*$' then 
                case
                    when duration_mins::numeric <= 0 then null
                    when duration_mins::numeric > 10000 then null
                    else duration_mins::numeric
                end
            else null
        end as duration_mins,
        
        -- Clean participants_count
        case
            when trim(participants_count::text) in ('', 'N/A', 'NULL', 'null') then null
            when participants_count::text ~ E'^-?\\d+$' then 
                case
                    when participants_count::integer <= 0 then null
                    when participants_count::integer > 5000 then null
                    else participants_count::integer
                end
            else null
        end as participants_count,
        
        -- Clean and parse features_used
        case
            when trim(features_used) in ('', 'null', 'n/a', 'NULL') then null
            when features_used like '%;%' then replace(lower(trim(features_used)), ';', ',')
            when features_used like '%|%' then replace(replace(lower(trim(features_used)), ' | ', ','), '|', ',')
            when features_used like '% %' then replace(lower(trim(features_used)), ' ', '_')
            else lower(trim(features_used))
        end as features_used,
        
        -- Parse individual feature flags
        case
            when lower(features_used) like '%screen_share%' or lower(features_used) like '%screen share%' then true
            else false
        end as used_screen_share,
        
        case
            when lower(features_used) like '%recording%' then true
            else false
        end as used_recording,
        
        case
            when lower(features_used) like '%breakout_rooms%' or lower(features_used) like '%breakout rooms%' then true
            else false
        end as used_breakout_rooms,
        
        case
            when lower(features_used) like '%polling%' then true
            else false
        end as used_polling,
        
        -- Flag for basic meetings
        case
            when trim(features_used) in ('', 'null', 'n/a', 'NULL') or features_used is null then true
            else false
        end as is_basic_meeting,
        
        current_timestamp as dbt_loaded_at

    from source
    where 
        trim(event_id) not in ('', 'NULL', 'N/A')
        and event_id is not null
)

select distinct * from cleaned