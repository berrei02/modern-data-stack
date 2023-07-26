with users as (
    select u.user_id
        , u.user_name
        , count(1) as subscription_months_cnt
        , count(1) filter (where not is_paid and is_overdue) as unpaid_subscriptions_cnt
        , sum(monthly_seat_price_eur) as lifetime_revenue
    from {{ ref("stg_users")}} as u
    inner join {{ ref("stg_subscriptions") }} as sub
       on u.user_id = sub.user_id
    inner join {{ ref("stg_subscription_pricing") }} as sub_price
       on sub.subscription = sub_price.package
      and sub_price.is_current_pricing
    group by 1, 2
)
select *
  from users