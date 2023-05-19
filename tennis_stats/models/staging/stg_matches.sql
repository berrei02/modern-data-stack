with stg_matches as (
    select tourney_id as tournament_id
         , tourney_name as tournament_name
         , date(tourney_date::varchar) as match_date
         , winner_id
         , winner_name
         , coalesce(winner_rank_points, 0) as winner_rank_points
         , winner_age
         , winner_hand
         , loser_id
         , loser_name
         , loser_age
         , loser_hand
         , coalesce(loser_rank_points, 0) as loser_rank_points
         , round as tournament_round
         , minutes as match_duration_minutes
      from {{ ref("matches") }}
)
select * 
  from stg_matches