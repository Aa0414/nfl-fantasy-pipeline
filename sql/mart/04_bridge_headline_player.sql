CREATE OR REPLACE TABLE mart.bridge_headline_player AS
WITH tag_matches AS (
  SELECT DISTINCT
    h.headline_key,
    p.player_id,
    'espn_tag' AS match_method,
    1.0 AS match_confidence
  FROM staging.stg_headline_athlete_tags t
  JOIN mart.fact_headline h ON h.link = t.link
  JOIN mart.dim_player p ON p.name_norm = t.name_norm
),
titles AS (
  SELECT
    headline_key,
    CONCAT(' ',
      TRIM(REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(LOWER(title), r"['’]s\b", ''),
            r"['’.]", ''),
          r'[^a-z0-9 -]', ' '),
        r'\s+', ' ')),
      ' ') AS title_clean
  FROM mart.fact_headline
),
name_matches AS (
  SELECT
    t.headline_key,
    p.player_id,
    'name_in_title' AS match_method,
    0.7 AS match_confidence
  FROM titles t
  JOIN mart.dim_player p
    ON STRPOS(t.title_clean, CONCAT(' ', p.name_norm, ' ')) > 0
)
SELECT * FROM tag_matches
UNION ALL
SELECT n.*
FROM name_matches n
WHERE NOT EXISTS (
  SELECT 1 FROM tag_matches tm
  WHERE tm.headline_key = n.headline_key AND tm.player_id = n.player_id
)
