-- norm_name: normalizes player names for matching (lowercase, strip periods/apostrophes, strip Jr/Sr/II/III/IV)

CREATE OR REPLACE FUNCTION staging.norm_name(x STRING) RETURNS STRING AS (
  TRIM(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(x), r"[.']", ''), r'\s+(jr|sr|ii|iii|iv)$', ''))
);
