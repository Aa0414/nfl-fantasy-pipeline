-- ESPN-tag headlines are a subset of all headlines, so the tag count
-- can never be larger than the total count. Returns offending rows.
select player_id, game_id, headline_count_pre_game, espn_tag_count_pre_game
from {{ ref('fact_player_week') }}
where espn_tag_count_pre_game > headline_count_pre_game
