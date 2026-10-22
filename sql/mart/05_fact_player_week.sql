CREATE OR REPLACE TABLE mart.fact_player_week AS
WITH cov AS (
  SELECT MIN(ingested_at_utc) AS news_coverage_start FROM mart.fact_headline
),
base AS (
  SELECT
    pw.player_id, pw.game_id, pw.season, pw.week, pw.team, pw.opponent_team,
    g.kickoff_ts_utc,
    pw.fantasy_points, pw.fantasy_points_ppr,
    AVG(pw.fantasy_points_ppr) OVER (
      PARTITION BY pw.player_id, pw.season ORDER BY pw.week
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS baseline_ppr_3g
  FROM staging.stg_player_week pw
  JOIN mart.dim_game g ON g.game_id = pw.game_id
),
counts AS (
  SELECT
    b.player_id, b.game_id,
    COUNT(DISTINCT br.headline_key) AS n_all,
    COUNT(DISTINCT IF(br.match_method = 'espn_tag', br.headline_key, NULL)) AS n_tag
  FROM base b
  JOIN mart.bridge_headline_player br ON br.player_id = b.player_id
  JOIN mart.fact_headline h ON h.headline_key = br.headline_key
  WHERE h.published_at_utc >= TIMESTAMP_SUB(b.kickoff_ts_utc, INTERVAL 7 DAY)
    AND h.published_at_utc <  b.kickoff_ts_utc
  GROUP BY 1, 2
)
SELECT
  b.player_id, b.game_id, b.season, b.week, b.team, b.opponent_team, b.kickoff_ts_utc,
  b.fantasy_points, b.fantasy_points_ppr, b.baseline_ppr_3g,
  b.fantasy_points_ppr - b.baseline_ppr_3g AS points_vs_baseline,
  CASE
    WHEN b.kickoff_ts_utc <= c.news_coverage_start THEN 'none'
    WHEN TIMESTAMP_SUB(b.kickoff_ts_utc, INTERVAL 7 DAY) < c.news_coverage_start THEN 'partial'
    ELSE 'full'
  END AS news_coverage,
  IF(b.kickoff_ts_utc <= c.news_coverage_start, NULL, IFNULL(n.n_all, 0)) AS headline_count_pre_game,
  IF(b.kickoff_ts_utc <= c.news_coverage_start, NULL, IFNULL(n.n_tag, 0)) AS espn_tag_count_pre_game
FROM base b
CROSS JOIN cov c
LEFT JOIN counts n ON n.player_id = b.player_id AND n.game_id = b.game_id
