WITH italian_customers AS (
    SELECT c.customer_id, c.customer_name
    FROM customers c
    JOIN countries co ON c.country_id = co.country_id
    WHERE co.country_name = 'Italy'
), dec2021_sales AS (
    SELECT
        s.customer_id,
        SUM(s.profit) AS total_profit
    FROM sales s
    JOIN times t ON s.time_id = t.time_id
    WHERE strftime('%Y-%m', t.date) = '2021-12'
    GROUP BY s.customer_id
), ranked_profits AS (
    SELECT
        ic.customer_id,
        ic.customer_name,
        COALESCE(ds.total_profit, 0) AS total_profit
    FROM italian_customers ic
    LEFT JOIN dec2021_sales ds ON ic.customer_id = ds.customer_id
), profit_tiers AS (
    SELECT
        customer_id,
        customer_name,
        total_profit,
        NTILE(10) OVER (ORDER BY total_profit DESC) AS tier
    FROM ranked_profits
)
SELECT
    tier,
    MIN(total_profit) AS min_profit,
    MAX(total_profit) AS max_profit
FROM profit_tiers
GROUP BY tier
ORDER BY tier;
