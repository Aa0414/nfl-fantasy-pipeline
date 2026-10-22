-- stg_player_week: one row per QB/RB/WR/TE per game week, with fantasy points
-- Depends on: raw.player_week_raw, staging.norm_name (00_norm_name.sql)

CREATE OR REPLACE TABLE staging.stg_player_week AS
SELECT * EXCEPT (rn)
FROM (
  SELECT
    TRIM(player_id) AS player_id,
    TRIM(player_display_name) AS player_name,
    staging.norm_name(player_display_name) AS name_norm,
    position,
    team,
    opponent_team,
    season,
    week,
    season_type,
    game_id,
    fantasy_points,
    fantasy_points_ppr,
    passing_yards, passing_tds, passing_interceptions,
    rushing_yards, rushing_tds, carries,
    receptions, targets, receiving_yards, receiving_tds,
    ROW_NUMBER() OVER (PARTITION BY TRIM(player_id), season, week ORDER BY game_id) AS rn
  FROM raw.player_week_raw
  WHERE position IN ('QB','RB','WR','TE')
    AND player_id IS NOT NULL
)
WHERE rn = 1
