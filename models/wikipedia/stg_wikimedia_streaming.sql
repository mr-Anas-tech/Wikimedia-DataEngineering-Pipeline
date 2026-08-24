with source_stream as (
    select * from {{ source('raw_source', 'WIKIMEDIA_STREAM_DATA') }}
)

select
    sequence_number,
    id as event_id,
    title as page_title,
    user as username,
    wiki as wiki_name,
    type as edit_type,
    bot as is_bot,
    to_timestamp(timestamp) as event_timestamp,
    coalesce(
        try_to_timestamp_ntz(enqueued_time, 'MM/DD/YYYY HH12:MI:SS AM'),
        try_to_timestamp_ntz(enqueued_time, 'M/D/YYYY HH12:MI:SS AM'),
        try_to_timestamp_ntz(enqueued_time, 'MM/DD/YYYY HH24:MI:SS'),
        try_to_timestamp_ntz(enqueued_time)
    ) as enqueued_timestamp
from source_stream