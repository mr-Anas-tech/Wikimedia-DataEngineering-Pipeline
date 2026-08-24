with streaming_events as (
    select
        md5(concat(cast(event_id as string), '_', coalesce(page_title, ''), '_', cast(event_timestamp as string))) as surrogate_key,
        event_id,
        page_title as article_title,
        'STREAMING_EDIT' as source_type,
        username,
        wiki_name,
        edit_type,
        is_bot,
        event_timestamp,
        null as view_count,
        null as pageview_period
    from {{ ref('stg_wikimedia_streaming') }}
),

batch_events as (
    select
        md5(concat(project, '_', article_title, '_', pageview_period)) as surrogate_key,
        null as event_id,
        article_title,
        'BATCH_PAGEVIEW' as source_type,
        null as username,
        project as wiki_name,
        access_method as edit_type,
        false as is_bot,
        to_timestamp(concat(pageview_period, '-01')) as event_timestamp,
        view_count,
        pageview_period
    from {{ ref('stg_wikimedia_batch') }}
),

combined_events as (
    select * from streaming_events
    union all
    select * from batch_events
),

deduplicated_events as (
    select
        *,
        row_number() over (
            partition by surrogate_key 
            order by coalesce(event_timestamp, current_timestamp()) desc
        ) as row_num
    from combined_events
)

-- Only keep unique records (row_num = 1)
select
    surrogate_key,
    event_id,
    article_title,
    source_type,
    username,
    wiki_name,
    edit_type,
    is_bot,
    event_timestamp,
    view_count,
    pageview_period
from deduplicated_events
where row_num = 1