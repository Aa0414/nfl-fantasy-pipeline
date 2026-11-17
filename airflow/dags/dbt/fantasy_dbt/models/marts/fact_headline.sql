select
    to_hex(md5(link)) as headline_key,
    link,
    title,
    description,
    source,
    published_at_utc,
    date(published_at_utc) as published_date_utc,
    ingested_at as ingested_at_utc
from {{ ref('stg_headlines') }}
