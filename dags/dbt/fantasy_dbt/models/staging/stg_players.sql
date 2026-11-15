select
cast(player_id as string) as sleeper_player_id,
cast(gsis_id as string) as gsis_id,
cast(espn_id as string) as espn_id,
cast(yahoo_id as string) as yahoo_id,
full_name, first_name, last_name, position, team,
status, injury_status, depth_chart_order, age, years_exp, snapshot_date
from {{ source('raw', 'players_sleeper_raw') }}
qualify row_number() over (partition by player_id order by snapshot_date desc) = 1
