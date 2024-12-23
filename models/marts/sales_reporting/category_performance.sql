WITH category_sales AS (
    SELECT
        p.category,
        SUM(s.quantity) AS total_sales,
        SUM(s.total_price) AS total_revenue,
        AVG(s.total_price / s.quantity) AS avg_price
    FROM {{ ref('stg_sales') }} s
    JOIN {{ ref('stg_products') }} p
        ON s.product_id = p.product_id
    GROUP BY p.category
)
SELECT
    category,
    total_sales,
    total_revenue,
    avg_price
FROM category_sales
ORDER BY total_revenue DESC;