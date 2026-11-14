WITH Italian_customers AS (
    SELECT
        c.customer_id,
        s.profit
    FROM
        customers c
    JOIN
        sales s ON c.customer_id = s.customer_id
    JOIN
        countries co ON c.country_id = co.country_id
    JOIN
        times t ON s.time_id = t.time_id
    WHERE
        co.country_name = 'Italy'
        AND t.year = 2021
        AND t.month = 12
),
Ranked_customers AS (
    SELECT
        customer_id,
        profit,
        NTILE(10) OVER (ORDER BY profit) AS profit_tier
    FROM
        Italian_customers
)
SELECT
    profit_tier,
    MIN(profit) AS lowest_profit,
    MAX(profit) AS highest_profit
FROM
    Ranked_customers
GROUP BY
    profit_tier
ORDER BY
    profit_tier;
