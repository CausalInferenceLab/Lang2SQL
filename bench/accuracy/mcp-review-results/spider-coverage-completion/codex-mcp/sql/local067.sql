WITH customer_profits AS (
    SELECT p.cust_id, SUM(p.amount_sold - p.total_cost) AS profit
    FROM main.profits AS p
    JOIN main.customers AS c ON c.cust_id = p.cust_id
    JOIN main.countries AS co ON co.country_id = c.country_id
    WHERE co.country_iso_code = 'IT'
      AND p.time_id >= '2021-12-01'
      AND p.time_id < '2022-01-01'
    GROUP BY p.cust_id
),
tiered_customers AS (
    SELECT profit, NTILE(10) OVER (ORDER BY profit DESC, cust_id) AS tier
    FROM customer_profits
)
SELECT tier, MAX(profit) AS highest_profit, MIN(profit) AS lowest_profit
FROM tiered_customers
GROUP BY tier
ORDER BY tier
LIMIT 10
