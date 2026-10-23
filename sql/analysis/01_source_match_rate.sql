-- Phase 1 snapshot: deduplicated headlines and player-match coverage by source.
-- Adjust the UTC date bounds for a later reporting window.
WITH headlines AS (
  SELECT headline_key, source
  FROM `ba882-team03-kalshi.mart.fact_headline`
  WHERE published_date_utc BETWEEN DATE '2026-10-19' AND DATE '2026-10-21'
),
matched AS (
  SELECT DISTINCT headline_key
  FROM `ba882-team03-kalshi.mart.bridge_headline_player`
  WHERE player_id IS NOT NULL
)
SELECT
  h.source,
  COUNT(DISTINCT h.headline_key) AS headline_count,
  COUNT(DISTINCT IF(m.headline_key IS NOT NULL, h.headline_key, NULL)) AS matched_headline_count,
  ROUND(
    100 * SAFE_DIVIDE(
      COUNT(DISTINCT IF(m.headline_key IS NOT NULL, h.headline_key, NULL)),
      COUNT(DISTINCT h.headline_key)
    ),
    1
  ) AS match_rate_pct
FROM headlines h
LEFT JOIN matched m USING (headline_key)
GROUP BY h.source
ORDER BY headline_count DESC;
