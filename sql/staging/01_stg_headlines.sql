   -- stg_headlines: ESPN + Google + Yahoo headlines, deduped by link, timestamps in UTC
   -- Depends on: raw.espn_news_raw, raw.google_news_raw, raw.yahoo_news_raw

-- build stg_headlines

CREATE OR REPLACE TABLE staging.stg_headlines AS
WITH unioned AS (
  SELECT
    TRIM(link) AS link,
    TRIM(title) AS title,
    TRIM(description) AS description,
    'espn' AS source,
    published AS published_at_utc,
    ingested_at
  FROM raw.espn_news_raw

  UNION ALL

  SELECT
    TRIM(link),
    TRIM(title),
    CAST(NULL AS STRING),
    'google',
    SAFE.PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S %z', REPLACE(TRIM(published), ' GMT', ' +0000')),
    SAFE_CAST(ingested_at AS TIMESTAMP)
  FROM raw.google_news_raw

  UNION ALL

  SELECT
    TRIM(link),
    TRIM(title),
    CAST(NULL AS STRING),
    'yahoo',
    SAFE.PARSE_TIMESTAMP('%a, %d %b %Y %H:%M:%S %z', REPLACE(TRIM(published), ' GMT', ' +0000')),
    ingested_at
  FROM raw.yahoo_news_raw
)
SELECT * EXCEPT (rn)
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY link ORDER BY ingested_at DESC) AS rn
  FROM unioned
  WHERE link IS NOT NULL AND title IS NOT NULL
)
WHERE rn = 1
