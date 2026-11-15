select
    game_id,
    season,
    week,
    game_type,
    gameday,
    gametime as gametime_et,
    kickoff_ts_utc,
    home_team,
    away_team,
    home_score,
    away_score,
    home_score is not null as is_played,
    spread_line,
    total_line,
    roof,
    surface
from {{ ref('stg_schedule') }}
