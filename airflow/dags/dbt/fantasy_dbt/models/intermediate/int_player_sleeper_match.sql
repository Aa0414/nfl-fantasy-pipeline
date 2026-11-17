-- One row per stats player, linked to a Sleeper player.
-- 1st choice: exact gsis_id match (stats player_id IS the gsis id)
-- 2nd choice: normalized name + team (Phase 1 method), only if no gsis match
with stats_players as (
    select
        player_id,
        array_agg(struct(player_name, name_norm, position, team)
                  order by season desc, week desc limit 1)[offset(0)] as latest
    from {{ ref('stg_player_week') }}
    group by player_id
),

sleeper as (
    select * from {{ ref('stg_players') }}
),

gsis_match as (
    select
        sp.player_id,
        s.sleeper_id, s.espn_id, s.status, s.injury_status, s.age, s.years_exp,
        'gsis_id' as sleeper_match_method
    from stats_players sp
    join sleeper s on s.gsis_id = sp.player_id
    qualify row_number() over (partition by sp.player_id order by s.snapshot_date desc) = 1
),

name_match as (
    select
        sp.player_id,
        s.sleeper_id, s.espn_id, s.status, s.injury_status, s.age, s.years_exp,
        'name_team' as sleeper_match_method
    from stats_players sp
    join sleeper s
        on s.name_norm = sp.latest.name_norm
       and s.team = sp.latest.team
    where sp.player_id not in (select player_id from gsis_match)
    qualify row_number() over (partition by sp.player_id order by s.snapshot_date desc) = 1
),

matched as (
    select * from gsis_match
    union all
    select * from name_match
)

select
    sp.player_id,
    m.sleeper_id,
    m.espn_id,
    m.status,
    m.injury_status,
    m.age,
    m.years_exp,
    coalesce(m.sleeper_match_method, 'unmatched') as sleeper_match_method
from stats_players sp
left join matched m using (player_id)
