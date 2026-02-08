-- models/staging/stg_accounts.sql

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

with source as (
    select * from {{ source('raw', 'accounts') }}
),

cleaned as (
    select
        trim(account_id) as account_id,
        initcap(trim(account_name)) as account_name,
        
        case
            when trim(lower(industry)) in ('', 'n/a', 'null', 'unknown') then null
            when trim(lower(industry)) in ('tech', 'technology') then 'Technology'
            when trim(lower(industry)) in ('healthcare', 'health care', 'helathcare') then 'Healthcare'
            when trim(lower(industry)) in ('edu', 'education', 'educaton') then 'Education'
            when trim(lower(industry)) in ('finance', 'financial services', 'fiance') then 'Finance'
            when trim(lower(industry)) = 'retail' then 'Retail'
            when trim(lower(industry)) in ('manufacturing', 'manufacuring') then 'Manufacturing'
            else initcap(trim(industry))
        end as industry,
        
        case
            when trim(created_at) in ('', 'N/A', 'NULL', 'Unknown') then null
            when created_at ~ '^\d{4}-\d{2}-\d{2}$' then created_at::date
            when created_at ~ '^\d{2}/\d{2}/\d{4}$' then to_date(created_at, 'MM/DD/YYYY')
            when created_at ~ '^\d{2}-\d{2}-\d{4}$' then to_date(created_at, 'DD-MM-YYYY')
            when created_at ~ '^\d{4}/\d{2}/\d{2}$' then to_date(replace(created_at, '/', '-'), 'YYYY-MM-DD')
            else null
        end as created_at,
        
        case
            when trim(account_size) in ('', 'N/A', 'NULL', 'null') then null
            when account_size::text ~ '^-?\d+$' then 
                case
                    when account_size::integer <= 0 then null
                    when account_size::integer > 50000 then null
                    else account_size::integer
                end
            else null
        end as account_size,
        
        current_timestamp as dbt_loaded_at,
        
        -- Add row number to identify duplicates (keep largest account_size)
        row_number() over (
            partition by trim(account_id) 
            order by 
                case
                    when account_size::text ~ '^-?\d+$' and account_size::integer > 0 
                    then account_size::integer 
                    else 0 
                end desc,
                created_at desc nulls last
        ) as row_num

    from source
    where 
        trim(account_id) not in ('', 'NULL', 'N/A')
        and account_id is not null
),

deduped as (
    select * from cleaned
    where row_num = 1  -- Keep only the first row per account_id
)

select
    account_id,
    account_name,
    industry,
    created_at,
    account_size,
    dbt_loaded_at
from deduped