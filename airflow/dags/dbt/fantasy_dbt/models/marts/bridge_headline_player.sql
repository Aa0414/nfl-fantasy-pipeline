select
    headline_key,
    player_id,
    match_method,
    match_confidence
from {{ ref('int_headline_player_matches') }}
