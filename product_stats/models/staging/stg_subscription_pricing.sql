with stg_subscription_pricing as (
    select package
         , valid_from::date
         , valid_until::date
         , valid_until is null or valid_until::date > now()::date as is_current_pricing
         , case when currency != 'eur'
               then null::numeric
                else monthly_seat_price::numeric
           end as monthly_seat_price_eur
      from {{ source("revenue_data", "pricing") }}
     order by 2 desc 
)
select * 
  from stg_subscription_pricing