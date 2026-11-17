-- Latest Sleeper snapshot per player
select
    player_id as sleeper_id,
    nullif(trim(gsis_id), '') as gsis_id,
    espn_id,
    yahoo_id,
    trim(full_name) as full_name,
    {{ norm_name('full_name') }} as name_norm,
    position,
    replace(trim(team), 'LAR', 'LA') as team,
    status,
    injury_status,
    depth_chart_order,
    age,
    years_exp,
    snapshot_date
from {{ source('raw', 'players_sleeper_raw') }}
where full_name is not null
qualify row_number() over (partition by player_id order by snapshot_date desc) = 1
