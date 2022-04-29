

with customers as (

    SELECT * from {{ ref('stg_customers') }}
),

orders as (

    SELECT * from {{ ref('stg_orders') }}

),


payments as (

    SELECT sum(case when payment.status = 'success' then payment.amount end) as fullamount,orders.customer_id
    from {{ ref('stg_payments') }} as payment
    left join orders  on (payment.order_id = orders.order_id)
    group by orders.customer_id
),


customer_orders as (

    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        payments.fullamount as lifetime_value

    from customers

    left join customer_orders using (customer_id)
    left join payments on (customers.customer_id = payments.customer_id)

)

select * from final