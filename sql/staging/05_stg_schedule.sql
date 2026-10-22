CREATE OR REPLACE TABLE staging.stg_schedule AS
SELECT * EXCEPT (rn)
FROM (
  SELECT
    game_id,
    season,
    week,
    game_type,
    gameday,
    gametime,
    SAFE.TIMESTAMP(CONCAT(CAST(gameday AS STRING), ' ', gametime, ':00'), 'America/New_York') AS kickoff_ts_utc,
    home_team,
    away_team,
    home_score,
    away_score,
    spread_line,
    total_line,
    roof,
    surface,
    ROW_NUMBER() OVER (PARTITION BY game_id ORDER BY home_score DESC NULLS LAST) AS rn
  FROM raw.schedule_raw
  WHERE game_id IS NOT NULL
)
WHERE rn = 1
