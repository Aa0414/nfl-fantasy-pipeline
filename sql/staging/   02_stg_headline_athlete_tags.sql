-- stg_headline_athlete_tags: one row per ESPN headline and tagged player name
-- Depends on: raw.espn_news_raw, staging.norm_name (00_norm_name.sql)

CREATE OR REPLACE TABLE staging.stg_headline_athlete_tags AS
SELECT
  TRIM(link) AS link,
  TRIM(t.label) AS label,
  staging.norm_name(t.label) AS name_norm,
  MAX(t.athlete_id) AS espn_athlete_id
FROM raw.espn_news_raw, UNNEST(tags) t
WHERE t.type = 'athlete'
  AND link IS NOT NULL
  AND t.label IS NOT NULL
GROUP BY 1, 2, 3
