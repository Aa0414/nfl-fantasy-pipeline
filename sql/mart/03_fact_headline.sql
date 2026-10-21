CREATE OR REPLACE TABLE mart.fact_headline AS
SELECT
  TO_HEX(MD5(link)) AS headline_key,
  link,
  title,
  description,
  source,
  published_at_utc,
  DATE(published_at_utc) AS published_date_utc,
  ingested_at AS ingested_at_utc
FROM staging.stg_headlines
