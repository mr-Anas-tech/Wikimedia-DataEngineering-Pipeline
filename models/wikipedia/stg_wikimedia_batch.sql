with source_batch as (
    select * from {{ source('raw_source', 'WIKIMEDIA_BATCH_DATA') }}
)

select
    project,
    article as article_title,
    access as access_method,
    year,
    month,
    day,
    concat(year, '-', lpad(month, 2, '0')) as pageview_period,
    views as view_count,
    rank
from source_batch