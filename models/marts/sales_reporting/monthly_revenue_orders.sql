WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', order_date) AS month,
        COUNT(order_id) AS total_orders,
        SUM(total_price) AS total_revenue
    FROM {{ ref('stg_sales') }}
    GROUP BY DATE_TRUNC('month', order_date)
)
SELECT
    TO_CHAR(month, 'YYYY-MM') AS month,
    total_orders,
    total_revenue
FROM monthly_sales
ORDER BY month;