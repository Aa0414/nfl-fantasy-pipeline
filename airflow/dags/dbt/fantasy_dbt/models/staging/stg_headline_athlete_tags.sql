-- One row per ESPN headline and tagged player name
select
    trim(link) as link,
    trim(t.label) as label,
    {{ norm_name('t.label') }} as name_norm,
    max(t.athlete_id) as espn_athlete_id
from {{ source('raw', 'espn_news_raw') }}, unnest(tags) as t
where t.type = 'athlete'
  and link is not null
  and t.label is not null
group by 1, 2, 3
