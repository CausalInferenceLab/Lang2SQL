WITH RECURSIVE
buckets(bucket) AS (
    VALUES (1)
    UNION ALL
    SELECT bucket + 1 FROM buckets WHERE bucket < 10
),
italian_customers AS (
    SELECT DISTINCT c.cust_id
    FROM customers AS c
    JOIN countries AS co ON co.country_id = c.country_id
    WHERE co.country_iso_code = 'IT'
),
december_sales AS MATERIALIZED (
    SELECT cust_id, prod_id, time_id, channel_id, promo_id, quantity_sold
    FROM sales
    WHERE time_id >= '2021-12-01' AND time_id < '2022-01-01'
),
december_costs AS MATERIALIZED (
    SELECT prod_id, time_id, channel_id, promo_id, unit_price, unit_cost
    FROM costs
    WHERE time_id >= '2021-12-01' AND time_id < '2022-01-01'
),
customer_profits AS (
    SELECT i.cust_id,
           COALESCE(SUM(s.quantity_sold * (d.unit_price - d.unit_cost)), 0.0) AS total_profit
    FROM italian_customers AS i
    LEFT JOIN december_sales AS s ON s.cust_id = i.cust_id
    LEFT JOIN december_costs AS d
        ON d.prod_id = s.prod_id
       AND d.time_id = s.time_id
       AND d.channel_id = s.channel_id
       AND d.promo_id = s.promo_id
    GROUP BY i.cust_id
),
profit_range AS (
    SELECT MIN(total_profit) AS min_profit, MAX(total_profit) AS max_profit
    FROM customer_profits
),
assigned AS (
    SELECT p.cust_id, p.total_profit,
           CASE
               WHEN r.min_profit = r.max_profit THEN 1
               ELSE MIN(10, 1 + CAST(10.0 * (p.total_profit - r.min_profit)
                                    / (r.max_profit - r.min_profit) AS INTEGER))
           END AS bucket
    FROM customer_profits AS p
    CROSS JOIN profit_range AS r
)
SELECT b.bucket,
       COUNT(a.cust_id) AS customer_count,
       MIN(a.total_profit) AS min_total_profit,
       MAX(a.total_profit) AS max_total_profit
FROM buckets AS b
LEFT JOIN assigned AS a ON a.bucket = b.bucket
GROUP BY b.bucket
ORDER BY b.bucket;
