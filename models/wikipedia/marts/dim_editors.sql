{{ config(
    materialized='table',
    snowflake_warehouse='COMPUTE_WH'
) }}

with base_events as (
    select * from {{ ref('int_wikimedia_events') }}
    where source_type = 'STREAMING_EDIT' and username is not null
)

select
    username,
    coalesce(max(is_bot), false) as is_bot,
    count(distinct article_title) as distinct_articles_edited,
    count(event_id) as total_edits_performed,
    min(event_timestamp) as first_active_timestamp,
    max(event_timestamp) as last_active_timestamp
from base_events
group by 1