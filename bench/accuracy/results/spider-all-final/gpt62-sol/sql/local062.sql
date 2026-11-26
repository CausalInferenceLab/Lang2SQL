WITH RECURSIVE
italian_customers AS (
    SELECT c.cust_id
    FROM customers AS c
    JOIN countries AS co ON co.country_id = c.country_id
    WHERE co.country_name = 'Italy'
),
december_profit AS (
    SELECT p.cust_id,
           SUM(p.quantity_sold * (p.unit_price - p.unit_cost)) AS total_profit
    FROM profits AS p
    JOIN italian_customers AS c ON c.cust_id = p.cust_id
    WHERE p.time_id >= '2021-12-01' AND p.time_id < '2022-01-01'
    GROUP BY p.cust_id
),
customer_profit AS (
    SELECT c.cust_id, COALESCE(p.total_profit, 0.0) AS total_profit
    FROM italian_customers AS c
    LEFT JOIN december_profit AS p ON p.cust_id = c.cust_id
),
profit_range AS (
    SELECT MIN(total_profit) AS min_profit, MAX(total_profit) AS max_profit
    FROM customer_profit
),
assigned AS (
    SELECT cp.total_profit,
           CASE
               WHEN r.min_profit = r.max_profit THEN 1
               ELSE MIN(10, 1 + CAST(10.0 * (cp.total_profit - r.min_profit)
                                    / (r.max_profit - r.min_profit) AS INTEGER))
           END AS bucket
    FROM customer_profit AS cp
    CROSS JOIN profit_range AS r
),
buckets(bucket) AS (
    SELECT 1
    UNION ALL
    SELECT bucket + 1 FROM buckets WHERE bucket < 10
)
SELECT b.bucket,
       COUNT(a.bucket) AS customer_count,
       MIN(a.total_profit) AS min_total_profit,
       MAX(a.total_profit) AS max_total_profit
FROM buckets AS b
LEFT JOIN assigned AS a ON a.bucket = b.bucket
GROUP BY b.bucket
ORDER BY b.bucket;
