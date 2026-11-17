-- One row per game, kickoff converted to UTC
select
    game_id,
    season,
    week,
    game_type,
    gameday,
    gametime,
    safe.timestamp(concat(cast(gameday as string), ' ', gametime, ':00'), 'America/New_York') as kickoff_ts_utc,
    home_team,
    away_team,
    home_score,
    away_score,
    spread_line,
    total_line,
    roof,
    surface
from {{ source('raw', 'schedule_raw') }}
where game_id is not null
qualify row_number() over (partition by game_id order by home_score desc nulls last) = 1
