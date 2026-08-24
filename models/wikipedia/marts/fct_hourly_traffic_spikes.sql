{{ config(
    materialized='table',
    snowflake_warehouse='COMPUTE_WH'
) }}

with base_events as (
    select * from {{ ref('int_wikimedia_events') }}
    where source_type = 'STREAMING_EDIT'
)

select
    date_trunc('hour', event_timestamp) as event_hour,
    wiki_name,
    edit_type,
    count(event_id) as edit_volume,
    count(distinct username) as active_contributors
from base_events
group by 1, 2, 3