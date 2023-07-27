with users as (
   select u.user_id
        , u.user_name
        , u.predicted_user_category
        , count(1) as subscription_months_cnt
        , count(1) filter (where not is_paid and is_overdue) as unpaid_subscriptions_cnt
        , sum(monthly_seat_price_eur) as lifetime_revenue
     from {{ ref("int_users")}} as u
    inner join {{ ref("stg_subscriptions") }} as sub
       on u.user_id = sub.user_id
    inner join {{ ref("stg_subscription_pricing") }} as sub_price
       on sub.subscription = sub_price.package
      and (
            (sub.start_date between sub_price.valid_from and sub_price.valid_until)
            or (sub_price.valid_until is null and sub.start_date > sub_price.valid_from)
          )
    group by 1, 2, 3
)
select *
  from users