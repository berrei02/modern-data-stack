with match_cluster_metadata as (
   select case when cluster is null
                then 'No Prediction (poor data)'
                else concat('Cluster ', cluster)
          end as cluster_name
        , avg(age_delta) as age_delta_avg
        , avg(rank_delta) as rank_delta_avg
        , count(1) as matches_cnt
    from {{ ref("stg_matches") }}
    left join {{ ref("int_similar_matches") }}
   using (match_id)
   group by 1
   order by 1
)
select *
  from match_cluster_metadata