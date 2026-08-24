{{ config(
    materialized='table',
    snowflake_warehouse='COMPUTE_WH'
) }}

with base_events as (
    select * from {{ ref('int_wikimedia_events') }}
)

select
    md5(concat(coalesce(article_title, 'UNKNOWN'), '_', cast(date_trunc('day', event_timestamp) as string))) as article_daily_key,
    article_title,
    wiki_name,
    date_trunc('day', event_timestamp) as metric_date,
    
    -- Streaming Metrics (Live Edits)
    count(case when source_type = 'STREAMING_EDIT' then 1 end) as total_edits,
    count(distinct case when source_type = 'STREAMING_EDIT' then username end) as unique_editors,
    count(case when source_type = 'STREAMING_EDIT' and is_bot = true then 1 end) as bot_edits,
    count(case when source_type = 'STREAMING_EDIT' and is_bot = false then 1 end) as human_edits,
    
    -- Batch Metrics (Pageviews)
    coalesce(sum(case when source_type = 'BATCH_PAGEVIEW' then view_count else 0 end), 0) as total_page_views

from base_events
where article_title is not null
group by 1, 2, 3, 4