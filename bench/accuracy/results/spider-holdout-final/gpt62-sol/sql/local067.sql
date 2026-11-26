WITH customer_profits AS (
    SELECT p.cust_id,
           SUM(p.amount_sold - p.total_cost) AS december_profit
    FROM profits AS p
    JOIN customers AS c ON c.cust_id = p.cust_id
    JOIN countries AS co ON co.country_id = c.country_id
    WHERE co.country_name = 'Italy'
      AND p.time_id >= '2021-12-01'
      AND p.time_id < '2022-01-01'
    GROUP BY p.cust_id
), tiered_customers AS (
    SELECT december_profit,
           NTILE(10) OVER (ORDER BY december_profit DESC, cust_id) AS tier
    FROM customer_profits
)
SELECT tier,
       MAX(december_profit) AS highest_profit,
       MIN(december_profit) AS lowest_profit
FROM tiered_customers
GROUP BY tier
ORDER BY tier;
