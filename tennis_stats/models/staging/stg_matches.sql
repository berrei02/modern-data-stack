with stg_matches as (
    select tourney_id as tournament_id
         , tourney_name as tournament_name
         , concat(tourney_name, '#', tourney_date, '#', winner_id, '#', loser_id) as match_id
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
         , case when winner_rank_points is null or loser_rank_points is null 
                then null
                else abs(winner_rank_points - loser_rank_points)
           end rank_delta
         , case when winner_age is null or loser_age is null 
                then null
                else abs(winner_age - loser_age)
           end age_delta
         , round as tournament_round
         , coalesce(minutes, 0) as match_duration_minutes
      from {{ ref("matches") }}
)
select * 
  from stg_matches