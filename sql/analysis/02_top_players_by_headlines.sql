-- Phase 1 snapshot: players with the most distinct matched headlines.
-- A headline can match multiple players; totals across players need not equal
-- the number of matched headlines in the source-level query.
WITH players AS (
  SELECT player_id, full_name, position, team
  FROM `ba882-team03-kalshi.mart.dim_player`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY player_id
    ORDER BY last_week_seen DESC, full_name
  ) = 1
)
SELECT
  b.player_id,
  COALESCE(p.full_name, b.player_id) AS player_name,
  p.position,
  p.team,
  COUNT(DISTINCT h.headline_key) AS matched_headline_count
FROM `ba882-team03-kalshi.mart.fact_headline` h
JOIN `ba882-team03-kalshi.mart.bridge_headline_player` b
  USING (headline_key)
LEFT JOIN players p
  USING (player_id)
WHERE h.published_date_utc BETWEEN DATE '2026-10-19' AND DATE '2026-10-21'
  AND b.player_id IS NOT NULL
GROUP BY b.player_id, player_name, p.position, p.team
ORDER BY matched_headline_count DESC, player_name
LIMIT 10;
