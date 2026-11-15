select
game_id, season, week, game_type, gameday, gametime, weekday,
home_team, away_team, home_score, away_score, result, overtime,
spread_line, total_line, roof, surface, temp, wind, div_game
from {{ source('raw', 'schedule_raw') }}
qualify row_number() over (partition by game_id order by gameday desc) = 1
