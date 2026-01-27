-- models/staging/stg_users.sql

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'users') }}
),

cleaned as (
    select
        -- Clean user_id
        trim(user_id) as user_id,
        
        -- Clean account_id
        trim(account_id) as account_id,
        
        -- Clean and validate email
        case
            when trim(lower(email)) in ('', 'invalid', 'test@', 'n/a', 'null') then null
            when email not like '%@%.%' then null  -- Basic email validation
            else lower(trim(email))
        end as email,
        
        -- Standardize role
        case
            when trim(lower(role)) in ('', 'null', 'n/a') then null
            when trim(lower(role)) in ('host', 'Host', 'HOST') then 'Host'
            when trim(lower(role)) in ('participant', 'Participant', 'PARTICIPANT') then 'Participant'
            when trim(lower(role)) in ('admin', 'Admin', 'ADMIN') then 'Admin'
            else initcap(trim(role))
        end as role,
        
        -- Parse created_at date
        case
            when trim(created_at) in ('', 'N/A', 'NULL', 'Unknown') then null
            when created_at ~ '^\d{4}-\d{2}-\d{2}$' then created_at::date
            when created_at ~ '^\d{2}/\d{2}/\d{4}$' then to_date(created_at, 'MM/DD/YYYY')
            when created_at ~ '^\d{2}-\d{2}-\d{4}$' then to_date(created_at, 'DD-MM-YYYY')
            when created_at ~ '^\d{4}/\d{2}/\d{2}$' then to_date(replace(created_at, '/', '-'), 'YYYY-MM-DD')
            else null
        end as created_at,
        
        -- Parse last_active_date
        case
            when trim(lower(last_active_date)) in ('', 'n/a', 'null', 'never', 'unknown') then null
            when last_active_date ~ '^\d{4}-\d{2}-\d{2}$' then last_active_date::date
            when last_active_date ~ '^\d{2}/\d{2}/\d{4}$' then to_date(last_active_date, 'MM/DD/YYYY')
            when last_active_date ~ '^\d{2}-\d{2}-\d{4}$' then to_date(last_active_date, 'DD-MM-YYYY')
            when last_active_date ~ '^\d{4}/\d{2}/\d{2}$' then to_date(replace(last_active_date, '/', '-'), 'YYYY-MM-DD')
            else null
        end as last_active_date,
        
        -- Flag for ghost users (never logged in)
        case
            when trim(lower(last_active_date)) in ('', 'n/a', 'null', 'never', 'unknown') then true
            else false
        end as is_ghost_user,
        
        current_timestamp as dbt_loaded_at

    from source
    where 
        trim(user_id) not in ('', 'NULL', 'N/A')
        and user_id is not null
)

select distinct * from cleaned