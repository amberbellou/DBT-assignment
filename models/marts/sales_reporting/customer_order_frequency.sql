WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(order_id) AS total_orders,
        MIN(order_date) AS first_order,
        MAX(order_date) AS last_order,
        (MAX(order_date) - MIN(order_date)) / NULLIF(COUNT(order_id) - 1, 0) AS avg_days_between_orders
    FROM {{ ref('stg_sales') }}
    GROUP BY customer_id
)
SELECT
    customer_id,
    total_orders,
    first_order,
    last_order,
    ROUND(avg_days_between_orders, 2) AS avg_days_between_orders
FROM customer_orders
ORDER BY total_orders DESC;