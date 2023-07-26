with stg_subscriptions as (
    select id as subscription_id
         , subscription
         , user_id
         , start_date
         , end_date
         , paid as is_paid
         , end_date < now()::date and not paid as is_overdue
      from {{ source("product_data", "subscriptions") }}
)
select *
  from stg_subscriptions