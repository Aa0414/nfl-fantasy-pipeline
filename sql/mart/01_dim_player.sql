CREATE OR REPLACE TABLE mart.dim_player AS
WITH stats_players AS (
  SELECT
    player_id,
    ARRAY_AGG(STRUCT(player_name, name_norm, position, team)
              ORDER BY season DESC, week DESC LIMIT 1)[OFFSET(0)] AS latest,
    MIN(week) AS first_week_seen,
    MAX(week) AS last_week_seen,
    COUNT(*) AS weeks_played
  FROM staging.stg_player_week
  GROUP BY player_id
)
SELECT
  sp.player_id,
  sp.latest.player_name AS full_name,
  sp.latest.name_norm AS name_norm,
  sp.latest.position AS position,
  sp.latest.team AS team,
  sl.sleeper_id,
  sl.espn_id,
  sl.status AS sleeper_status,
  sl.injury_status,
  sl.age,
  sl.years_exp,
  sp.first_week_seen,
  sp.last_week_seen,
  sp.weeks_played
FROM stats_players sp
LEFT JOIN staging.stg_players sl
  ON sl.name_norm = sp.latest.name_norm
 AND sl.team = sp.latest.team
