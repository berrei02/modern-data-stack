with stg_users as (
    select user_id
         , name as user_name
      from {{ source("product_data", "users") }}
)
select * 
  from stg_users