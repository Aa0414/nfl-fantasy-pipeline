with unioned as (
select 'espn' as provider, cast(title as string) as title, cast(link as string) as link,
cast(description as string) as description, cast(published as string) as published_raw,
cast(source as string) as source, safe_cast(ingested_at as timestamp) as ingested_at
from {{ source('raw', 'espn_news_raw') }}
union all
select 'google', cast(title as string), cast(link as string), cast(null as string),
cast(published as string), cast(source as string), safe_cast(ingested_at as timestamp)
from {{ source('raw', 'google_news_raw') }}
union all
select 'yahoo', cast(title as string), cast(link as string), cast(null as string),
cast(published as string), cast(source as string), safe_cast(ingested_at as timestamp)
from {{ source('raw', 'yahoo_news_raw') }}
)
select
to_hex(md5(link)) as headline_id,
provider, title, description, link, source,
coalesce(safe_cast(published_raw as timestamp),
safe.parse_timestamp('%a, %d %b %Y %H:%M:%S %Z', published_raw)) as published_at,
ingested_at
from unioned
where link is not null
qualify row_number() over (partition by link order by ingested_at) = 1
