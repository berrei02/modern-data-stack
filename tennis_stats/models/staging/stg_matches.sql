with stg_matches as (
    select tourney_id as tournament_id
         , tourney_name as tournament_name
         , date(tourney_date::varchar) as match_date
         , winner_id
         , winner_name
         , round as tournament_round
         , minutes as match_duration_minutes
         , winner_rank_points
         , loser_rank_points
      from {{ ref("matches") }}
)
select * 
  from stg_matches