select
cast(player_id as string) as gsis_id,
player_name, player_display_name, position, position_group,
team, opponent_team, game_id, season, week, season_type,
fantasy_points, fantasy_points_ppr,
completions, attempts, passing_yards, passing_tds, passing_interceptions,
carries, rushing_yards, rushing_tds,
targets, receptions, receiving_yards, receiving_tds, target_share
from {{ source('raw', 'player_week_raw') }}
qualify row_number() over (
partition by player_id, season, week, season_type order by fantasy_points_ppr desc) = 1
