WITH buckets(bucket) AS (
    VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)
),
customer_profits AS (
    SELECT c.cust_id,
           COALESCE(SUM(
               s.quantity_sold * CAST(ROUND(100 * (k.unit_price - k.unit_cost)) AS INTEGER)
           ), 0) AS profit_cents
    FROM main.customers AS c
    JOIN main.countries AS n ON n.country_id = c.country_id
    LEFT JOIN main.sales AS s
        ON s.cust_id = c.cust_id
       AND s.time_id >= '2021-12-01'
       AND s.time_id < '2022-01-01'
    LEFT JOIN main.costs AS k
        ON k.prod_id = s.prod_id
       AND k.time_id = s.time_id
       AND k.channel_id = s.channel_id
       AND k.promo_id = s.promo_id
    WHERE n.country_iso_code = 'IT'
    GROUP BY c.cust_id
),
bounds AS (
    SELECT MIN(profit_cents) AS low, MAX(profit_cents) AS high
    FROM customer_profits
),
assigned AS (
    SELECT p.cust_id, p.profit_cents,
           CASE
               WHEN r.high = r.low THEN 1
               WHEN p.profit_cents = r.high THEN 10
               ELSE 1 + 10 * (p.profit_cents - r.low) / (r.high - r.low)
           END AS bucket
    FROM customer_profits AS p
    CROSS JOIN bounds AS r
)
SELECT b.bucket,
       COUNT(a.cust_id) AS customer_count,
       MIN(a.profit_cents) / 100.0 AS min_total_profit,
       MAX(a.profit_cents) / 100.0 AS max_total_profit
FROM buckets AS b
LEFT JOIN assigned AS a ON a.bucket = b.bucket
GROUP BY b.bucket
ORDER BY b.bucket
LIMIT 10
