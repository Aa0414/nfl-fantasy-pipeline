CREATE OR REPLACE TABLE staging.stg_players AS
SELECT * EXCEPT (rn)
FROM (
  SELECT
    player_id AS sleeper_id,
    NULLIF(TRIM(gsis_id), '') AS gsis_id,
    espn_id,
    yahoo_id,
    TRIM(full_name) AS full_name,
    staging.norm_name(full_name) AS name_norm,
    position,
    REPLACE(TRIM(team), 'LAR', 'LA') AS team,
    status,
    injury_status,
    depth_chart_order,
    age,
    years_exp,
    snapshot_date,
    ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY snapshot_date DESC) AS rn
  FROM raw.players_sleeper_raw
  WHERE full_name IS NOT NULL
)
WHERE rn = 1
