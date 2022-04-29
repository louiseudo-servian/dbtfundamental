with customers as (

    SELECT * from {{ ref('stg_customers') }}
),
 payments as (

    SELECT * from {{ ref('stg_payments') }}
),
 orders as (

    SELECT * from {{ ref('stg_orders') }}

),

final as (

    select
        orders.order_id,
        customers.customer_id,
        payments.amount

    from orders

    left join payments using (order_id)

    left join customers using (customer_id)
)

select * from final