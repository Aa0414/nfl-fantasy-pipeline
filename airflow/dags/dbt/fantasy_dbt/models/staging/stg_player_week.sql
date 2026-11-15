-- One row per QB/RB/WR/TE per game week, with fantasy points
select
    trim(player_id) as player_id,
    trim(player_display_name) as player_name,
    {{ norm_name('player_display_name') }} as name_norm,
    position,
    team,
    opponent_team,
    season,
    week,
    season_type,
    game_id,
    fantasy_points,
    fantasy_points_ppr,
    passing_yards, passing_tds, passing_interceptions,
    rushing_yards, rushing_tds, carries,
    receptions, targets, receiving_yards, receiving_tds
from {{ source('raw', 'player_week_raw') }}
where position in ('QB', 'RB', 'WR', 'TE')
  and player_id is not null
qualify row_number() over (partition by trim(player_id), season, week order by game_id) = 1
