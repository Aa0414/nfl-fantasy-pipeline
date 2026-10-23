-- Descriptive Week 6 performance only: no news-to-points inference is possible
-- without a complete seven-day pregame news window.
WITH players AS (
  SELECT player_id, full_name, position
  FROM `ba882-team03-kalshi.mart.dim_player`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY player_id
    ORDER BY last_week_seen DESC, full_name
  ) = 1
)
SELECT
  f.player_id,
  COALESCE(p.full_name, f.player_id) AS player_name,
  p.position,
  f.team,
  ROUND(f.fantasy_points_ppr, 2) AS fantasy_points_ppr,
  ROUND(f.baseline_ppr_3g, 2) AS baseline_ppr_3g,
  ROUND(f.points_vs_baseline, 2) AS points_vs_baseline
FROM `ba882-team03-kalshi.mart.fact_player_week` f
LEFT JOIN players p
  USING (player_id)
WHERE f.season = 2026
  AND f.week = 6
  AND f.fantasy_points_ppr IS NOT NULL
  AND f.baseline_ppr_3g IS NOT NULL
  AND p.position IN ('QB', 'RB', 'WR', 'TE')
ORDER BY points_vs_baseline DESC, player_name
LIMIT 20;
