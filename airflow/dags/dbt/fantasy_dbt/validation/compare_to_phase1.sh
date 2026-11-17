#!/usr/bin/env bash
# Compares dbt output with the Phase 1 tables in `mart`.
# Usage: bash validation/compare_to_phase1.sh [dbt_dataset]   (default: dbt_allen)
# Only compares data Phase 1 had (headlines it saw, weeks it covered), so new
# raw data arriving since Phase 1 does not cause false mismatches.
DS="${1:-dbt_allen}"
P="ba882-team03-kalshi"
bq query --use_legacy_sql=false --format=pretty "
DECLARE max_sw INT64 DEFAULT (SELECT MAX(season * 100 + week) FROM \`$P.mart.fact_player_week\`);

WITH checks AS (
  SELECT 1 AS n, 'fact_headline: Phase 1 headlines present in dbt' AS check_name, 'equal' AS expect,
    CAST((SELECT COUNT(*) FROM \`$P.mart.fact_headline\`) AS FLOAT64) AS phase1,
    CAST((SELECT COUNT(*) FROM \`$P.mart.fact_headline\` o
          JOIN \`$P.$DS.fact_headline\` d USING (headline_key)) AS FLOAT64) AS dbt
  UNION ALL
  SELECT 2, 'bridge: Phase 1 headline-player pairs present in dbt', 'equal',
    CAST((SELECT COUNT(*) FROM (SELECT DISTINCT headline_key, player_id FROM \`$P.mart.bridge_headline_player\`)) AS FLOAT64),
    CAST((SELECT COUNT(*) FROM (SELECT DISTINCT headline_key, player_id FROM \`$P.mart.bridge_headline_player\`) o
          JOIN \`$P.$DS.bridge_headline_player\` d USING (headline_key, player_id)) AS FLOAT64)
  UNION ALL
  SELECT 3, 'dim_game: rows', 'equal',
    CAST((SELECT COUNT(*) FROM \`$P.mart.dim_game\`) AS FLOAT64),
    CAST((SELECT COUNT(*) FROM \`$P.$DS.dim_game\`) AS FLOAT64)
  UNION ALL
  SELECT 4, 'dim_player: distinct players', 'equal',
    CAST((SELECT COUNT(DISTINCT player_id) FROM \`$P.mart.dim_player\`) AS FLOAT64),
    CAST((SELECT COUNT(DISTINCT player_id) FROM \`$P.$DS.dim_player\`) AS FLOAT64)
  UNION ALL
  SELECT 5, 'dim_player: duplicate player rows', 'lower',
    CAST((SELECT COUNT(*) - COUNT(DISTINCT player_id) FROM \`$P.mart.dim_player\`) AS FLOAT64),
    CAST((SELECT COUNT(*) - COUNT(DISTINCT player_id) FROM \`$P.$DS.dim_player\`) AS FLOAT64)
  UNION ALL
  SELECT 6, 'dim_player: players linked to Sleeper', 'higher',
    CAST((SELECT COUNT(DISTINCT IF(sleeper_id IS NOT NULL, player_id, NULL)) FROM \`$P.mart.dim_player\`) AS FLOAT64),
    CAST((SELECT COUNT(DISTINCT IF(sleeper_id IS NOT NULL, player_id, NULL)) FROM \`$P.$DS.dim_player\`) AS FLOAT64)
  UNION ALL
  SELECT 7, 'fact_player_week: rows (Phase 1 weeks)', 'equal',
    CAST((SELECT COUNT(*) FROM \`$P.mart.fact_player_week\`) AS FLOAT64),
    CAST((SELECT COUNT(*) FROM \`$P.$DS.fact_player_week\` WHERE season * 100 + week <= max_sw) AS FLOAT64)
  UNION ALL
  SELECT 8, 'fact_player_week: total PPR points (Phase 1 weeks)', 'equal',
    ROUND((SELECT SUM(fantasy_points_ppr) FROM \`$P.mart.fact_player_week\`), 2),
    ROUND((SELECT SUM(fantasy_points_ppr) FROM \`$P.$DS.fact_player_week\` WHERE season * 100 + week <= max_sw), 2)
  UNION ALL
  SELECT 9, 'fact_player_week: total pregame headlines (Phase 1 weeks)', 'equal_or_higher',
    CAST((SELECT SUM(headline_count_pre_game) FROM \`$P.mart.fact_player_week\`) AS FLOAT64),
    CAST((SELECT SUM(headline_count_pre_game) FROM \`$P.$DS.fact_player_week\` WHERE season * 100 + week <= max_sw) AS FLOAT64)
)
SELECT
  check_name, phase1, dbt,
  CASE
    WHEN expect = 'equal'           AND phase1 = dbt  THEN 'MATCH'
    WHEN expect = 'equal_or_higher' AND dbt >= phase1 THEN 'MATCH'
    WHEN expect = 'higher'          AND dbt >= phase1 THEN 'IMPROVED'
    WHEN expect = 'lower'           AND dbt <= phase1 THEN 'IMPROVED'
    ELSE 'CHECK'
  END AS result
FROM checks
ORDER BY n
"
