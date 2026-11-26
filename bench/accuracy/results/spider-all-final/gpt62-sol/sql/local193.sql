WITH purchases AS (
    SELECT customer_id,
           amount,
           unixepoch(payment_date) AS purchase_second
    FROM payment
), timed_purchases AS (
    SELECT customer_id,
           amount,
           purchase_second - MIN(purchase_second) OVER (PARTITION BY customer_id) AS elapsed_seconds
    FROM purchases
), customer_ltv AS (
    SELECT customer_id,
           SUM(amount) AS total_ltv,
           SUM(CASE WHEN elapsed_seconds < 7 * 86400 THEN amount ELSE 0 END) AS sales_7_days,
           SUM(CASE WHEN elapsed_seconds < 30 * 86400 THEN amount ELSE 0 END) AS sales_30_days
    FROM timed_purchases
    GROUP BY customer_id
    HAVING SUM(amount) <> 0
)
SELECT AVG(100.0 * sales_7_days / total_ltv) AS avg_ltv_pct_first_7_days,
       AVG(100.0 * sales_30_days / total_ltv) AS avg_ltv_pct_first_30_days,
       AVG(total_ltv) AS avg_total_ltv
FROM customer_ltv;
