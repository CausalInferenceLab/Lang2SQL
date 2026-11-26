WITH order_spend AS (
    -- Sum all payment rows once per order, including split payments.
    SELECT order_id, SUM(payment_value) AS spend
    FROM order_payments
    GROUP BY order_id
), customer_totals AS (
    SELECT c.customer_unique_id,
           MAX(o.order_purchase_timestamp) AS latest_purchase,
           COUNT(*) AS order_count,
           -- Missing payments leave spend unknown, not zero.
           CASE WHEN COUNT(p.spend) = COUNT(*)
                THEN ROUND(SUM(p.spend), 2)
           END AS total_spend
    FROM orders AS o
    JOIN customers AS c ON c.customer_id = o.customer_id
    LEFT JOIN order_spend AS p ON p.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
), recency AS (
    -- Recency is elapsed days from the customer's latest delivered-order purchase
    -- to the latest delivered-order purchase in the dataset. Lower is more recent.
    SELECT *,
           JULIANDAY(MAX(latest_purchase) OVER ())
               - JULIANDAY(latest_purchase) AS recency_days
    FROM customer_totals
), percentiles AS (
    -- Rank each measure best to worst. Tied values share the cumulative percentile
    -- at the end of their tie group. Unknown spend receives no monetary percentile.
    SELECT *,
           CUME_DIST() OVER (ORDER BY recency_days) AS r_pct,
           CUME_DIST() OVER (ORDER BY order_count DESC) AS f_pct,
           CASE WHEN total_spend IS NOT NULL THEN
               CUME_DIST() OVER (
                   PARTITION BY (total_spend IS NULL) ORDER BY total_spend DESC
               )
           END AS m_pct
    FROM recency
), scores AS (
    -- The percentile bands end at 20%, 40%, 60%, 80%, and 100%.
    SELECT *,
           CASE WHEN r_pct <= 0.2 THEN 1 WHEN r_pct <= 0.4 THEN 2
                WHEN r_pct <= 0.6 THEN 3 WHEN r_pct <= 0.8 THEN 4 ELSE 5 END AS r_score,
           CASE WHEN f_pct <= 0.2 THEN 1 WHEN f_pct <= 0.4 THEN 2
                WHEN f_pct <= 0.6 THEN 3 WHEN f_pct <= 0.8 THEN 4 ELSE 5 END
           + CASE WHEN m_pct IS NULL THEN NULL
                  WHEN m_pct <= 0.2 THEN 1 WHEN m_pct <= 0.4 THEN 2
                  WHEN m_pct <= 0.6 THEN 3 WHEN m_pct <= 0.8 THEN 4 ELSE 5 END AS fm_score
    FROM percentiles
), segments AS (
    SELECT customer_unique_id, total_spend, order_count,
           CASE
               WHEN r_score = 1 AND fm_score BETWEEN 1 AND 4 THEN 'Champions'
               WHEN r_score IN (4, 5) AND fm_score BETWEEN 1 AND 2 THEN 'Can''t Lose Them'
               WHEN r_score IN (4, 5) AND fm_score BETWEEN 3 AND 6 THEN 'Hibernating'
               WHEN r_score IN (4, 5) AND fm_score BETWEEN 7 AND 10 THEN 'Lost'
               WHEN r_score IN (2, 3) AND fm_score BETWEEN 1 AND 4 THEN 'Loyal Customers'
               WHEN r_score = 3 AND fm_score BETWEEN 5 AND 6 THEN 'Needs Attention'
               WHEN r_score = 1 AND fm_score BETWEEN 7 AND 8 THEN 'Recent Users'
               WHEN (r_score = 1 AND fm_score BETWEEN 5 AND 6)
                 OR (r_score = 2 AND fm_score BETWEEN 5 AND 8) THEN 'Potential Loyalists'
               WHEN r_score = 1 AND fm_score BETWEEN 9 AND 10 THEN 'Price Sensitive'
               WHEN r_score = 2 AND fm_score BETWEEN 9 AND 10 THEN 'Promising'
               WHEN r_score = 3 AND fm_score BETWEEN 7 AND 10 THEN 'About to Sleep'
           END AS rfm_segment
    FROM scores
)
SELECT customer_unique_id,
       rfm_segment,
       ROUND(total_spend / order_count, 2) AS customer_avg_sales_per_order,
       -- Compare segments using total segment spend divided by total segment orders.
       ROUND(SUM(total_spend) OVER (PARTITION BY rfm_segment)
             / SUM(order_count) OVER (PARTITION BY rfm_segment), 2) AS segment_avg_sales_per_order
FROM segments
ORDER BY segment_avg_sales_per_order DESC, rfm_segment, customer_unique_id;
