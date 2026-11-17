with cov as (
    select min(ingested_at_utc) as news_coverage_start
    from {{ ref('fact_headline') }}
),

base as (
    select
        pw.player_id, pw.game_id, pw.season, pw.week, pw.team, pw.opponent_team,
        g.kickoff_ts_utc,
        pw.fantasy_points, pw.fantasy_points_ppr,
        avg(pw.fantasy_points_ppr) over (
            partition by pw.player_id, pw.season order by pw.week
            rows between 3 preceding and 1 preceding) as baseline_ppr_3g
    from {{ ref('stg_player_week') }} pw
    join {{ ref('dim_game') }} g on g.game_id = pw.game_id
),

counts as (
    select
        b.player_id,
        b.game_id,
        count(distinct br.headline_key) as n_all,
        count(distinct if(br.match_method = 'espn_tag', br.headline_key, null)) as n_tag
    from base b
    join {{ ref('bridge_headline_player') }} br on br.player_id = b.player_id
    join {{ ref('fact_headline') }} h on h.headline_key = br.headline_key
    where h.published_at_utc >= timestamp_sub(b.kickoff_ts_utc, interval 7 day)
      and h.published_at_utc <  b.kickoff_ts_utc
    group by 1, 2
)

select
    b.player_id, b.game_id, b.season, b.week, b.team, b.opponent_team, b.kickoff_ts_utc,
    b.fantasy_points, b.fantasy_points_ppr, b.baseline_ppr_3g,
    b.fantasy_points_ppr - b.baseline_ppr_3g as points_vs_baseline,
    case
        when b.kickoff_ts_utc <= c.news_coverage_start then 'none'
        when timestamp_sub(b.kickoff_ts_utc, interval 7 day) < c.news_coverage_start then 'partial'
        else 'full'
    end as news_coverage,
    if(b.kickoff_ts_utc <= c.news_coverage_start, null, ifnull(n.n_all, 0)) as headline_count_pre_game,
    if(b.kickoff_ts_utc <= c.news_coverage_start, null, ifnull(n.n_tag, 0)) as espn_tag_count_pre_game
from base b
cross join cov c
left join counts n
    on n.player_id = b.player_id
   and n.game_id = b.game_id
