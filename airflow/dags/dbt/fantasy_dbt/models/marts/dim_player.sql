with stats_players as (
    select
        player_id,
        array_agg(struct(player_name, name_norm, position, team)
                  order by season desc, week desc limit 1)[offset(0)] as latest,
        min(week) as first_week_seen,
        max(week) as last_week_seen,
        count(*) as weeks_played
    from {{ ref('stg_player_week') }}
    group by player_id
)

select
    sp.player_id,
    sp.latest.player_name as full_name,
    sp.latest.name_norm as name_norm,
    sp.latest.position as position,
    sp.latest.team as team,
    m.sleeper_id,
    m.espn_id,
    m.status as sleeper_status,
    m.injury_status,
    m.age,
    m.years_exp,
    m.sleeper_match_method,
    sp.first_week_seen,
    sp.last_week_seen,
    sp.weeks_played
from stats_players sp
left join {{ ref('int_player_sleeper_match') }} m using (player_id)
