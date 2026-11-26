WITH delivered_orders AS (
    SELECT o.order_id, c.customer_unique_id, o.order_purchase_timestamp
    FROM orders AS o
    JOIN customers AS c ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
),
order_spend AS (
    -- Sum all recorded payments for an order before counting orders.
    SELECT p.order_id,
           CASE WHEN COUNT(p.payment_value) = COUNT(*)
                THEN SUM(p.payment_value) END AS spend
    FROM order_payments AS p
    JOIN delivered_orders AS d ON d.order_id = p.order_id
    GROUP BY p.order_id
),
customer_totals AS (
    SELECT d.customer_unique_id,
           MAX(d.order_purchase_timestamp) AS latest_purchase_timestamp,
           COUNT(*) AS order_count,
           -- Keep every delivered order; spend is unknown if any order lacks payment data.
           CASE WHEN COUNT(p.spend) = COUNT(*)
                THEN ROUND(SUM(p.spend), 2) END AS total_spend
    FROM delivered_orders AS d
    LEFT JOIN order_spend AS p ON p.order_id = d.order_id
    GROUP BY d.customer_unique_id
),
customer_rfm AS (
    -- Recency is elapsed days from the customer's latest delivered-order purchase
    -- to the latest delivered-order purchase in the full dataset.
    SELECT *,
           JULIANDAY(MAX(latest_purchase_timestamp) OVER ())
               - JULIANDAY(latest_purchase_timestamp) AS recency_days
    FROM customer_totals
),
percentiles AS (
    -- Rank best to worst. CUME_DIST keeps equal values in the same percentile band.
    SELECT *,
           CUME_DIST() OVER (ORDER BY recency_days) AS r_pct,
           CUME_DIST() OVER (ORDER BY order_count DESC) AS f_pct,
           CUME_DIST() OVER (
               PARTITION BY total_spend IS NULL ORDER BY total_spend DESC
           ) AS m_pct
    FROM customer_rfm
),
scores AS (
    -- Scores 1–5 use the 20%, 40%, 60%, and 80% cumulative percentile cutoffs.
    SELECT customer_unique_id, order_count, total_spend,
           1 + (r_pct > 0.2) + (r_pct > 0.4) + (r_pct > 0.6) + (r_pct > 0.8) AS r,
           1 + (f_pct > 0.2) + (f_pct > 0.4) + (f_pct > 0.6) + (f_pct > 0.8)
             + CASE WHEN total_spend IS NOT NULL THEN
                   1 + (m_pct > 0.2) + (m_pct > 0.4) + (m_pct > 0.6) + (m_pct > 0.8)
               END AS fm
    FROM percentiles
),
segmented AS (
    SELECT customer_unique_id, order_count, total_spend,
           CASE
               WHEN r = 1 AND fm BETWEEN 1 AND 4 THEN 'Champions'
               WHEN r IN (4, 5) AND fm BETWEEN 1 AND 2 THEN 'Can''t Lose Them'
               WHEN r IN (4, 5) AND fm BETWEEN 3 AND 6 THEN 'Hibernating'
               WHEN r IN (4, 5) AND fm BETWEEN 7 AND 10 THEN 'Lost'
               WHEN r IN (2, 3) AND fm BETWEEN 1 AND 4 THEN 'Loyal Customers'
               WHEN r = 3 AND fm BETWEEN 5 AND 6 THEN 'Needs Attention'
               WHEN r = 1 AND fm BETWEEN 7 AND 8 THEN 'Recent Users'
               WHEN (r = 1 AND fm BETWEEN 5 AND 6)
                 OR (r = 2 AND fm BETWEEN 5 AND 8) THEN 'Potential Loyalists'
               WHEN r = 1 AND fm BETWEEN 9 AND 10 THEN 'Price Sensitive'
               WHEN r = 2 AND fm BETWEEN 9 AND 10 THEN 'Promising'
               WHEN r = 3 AND fm BETWEEN 7 AND 10 THEN 'About to Sleep'
               ELSE 'Unclassified (missing spend)'
           END AS rfm_segment
    FROM scores
)
SELECT customer_unique_id,
       rfm_segment,
       ROUND(total_spend / order_count, 2) AS avg_sales_per_order,
       -- Compare segments by pooled spend / pooled orders, not by averaging customer ratios.
       ROUND(SUM(total_spend) OVER segment / SUM(order_count) OVER segment, 2)
           AS segment_avg_sales_per_order
FROM segmented
WINDOW segment AS (PARTITION BY rfm_segment)
ORDER BY segment_avg_sales_per_order DESC, rfm_segment, customer_unique_id;
