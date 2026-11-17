-- Headline -> player matches (Phase 1 logic):
--   espn_tag      : ESPN athlete tag name = player name   (confidence 1.0)
--   name_in_title : player name appears in the headline   (confidence 0.7)
-- A name_in_title match is dropped if the same pair already matched by tag.
with players as (
    select player_id, name_norm from {{ ref('dim_player') }}
),

tag_matches as (
    select distinct
        h.headline_key,
        p.player_id,
        'espn_tag' as match_method,
        1.0 as match_confidence
    from {{ ref('stg_headline_athlete_tags') }} t
    join {{ ref('fact_headline') }} h on h.link = t.link
    join players p on p.name_norm = t.name_norm
),

titles as (
    select
        headline_key,
        concat(' ',
            trim(regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(lower(title), r"['’]s\b", ''),
                        r"['’.]", ''),
                    r'[^a-z0-9 -]', ' '),
                r'\s+', ' ')),
            ' ') as title_clean
    from {{ ref('fact_headline') }}
),

name_matches as (
    select distinct
        t.headline_key,
        p.player_id,
        'name_in_title' as match_method,
        0.7 as match_confidence
    from titles t
    join players p
        on strpos(t.title_clean, concat(' ', p.name_norm, ' ')) > 0
)

select * from tag_matches
union all
select n.*
from name_matches n
where not exists (
    select 1 from tag_matches tm
    where tm.headline_key = n.headline_key
      and tm.player_id = n.player_id
)
