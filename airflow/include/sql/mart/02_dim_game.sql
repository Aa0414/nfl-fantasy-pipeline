CREATE OR REPLACE TABLE mart.dim_game AS
SELECT
  game_id,
  season,
  week,
  game_type,
  gameday,
  gametime AS gametime_et,
  kickoff_ts_utc,
  home_team,
  away_team,
  home_score,
  away_score,
  home_score IS NOT NULL AS is_played,
  spread_line,
  total_line,
  roof,
  surface
FROM staging.stg_schedule
