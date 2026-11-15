-- ESPN + Google + Yahoo headlines, deduped by link, timestamps in UTC
with unioned as (
    select
        trim(link) as link,
        trim(title) as title,
        trim(description) as description,
        'espn' as source,
        published as published_at_utc,
        ingested_at
    from {{ source('raw', 'espn_news_raw') }}

    union all

    select
        trim(link),
        trim(title),
        cast(null as string),
        'google',
        safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %z', replace(trim(published), ' GMT', ' +0000')),
        safe_cast(ingested_at as timestamp)
    from {{ source('raw', 'google_news_raw') }}

    union all

    select
        trim(link),
        trim(title),
        cast(null as string),
        'yahoo',
        safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %z', replace(trim(published), ' GMT', ' +0000')),
        ingested_at
    from {{ source('raw', 'yahoo_news_raw') }}
)

select *
from unioned
where link is not null
  and title is not null
qualify row_number() over (partition by link order by ingested_at desc) = 1
