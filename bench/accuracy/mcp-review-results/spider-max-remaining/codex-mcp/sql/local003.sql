WITH payments_by_order AS (
    SELECT p.order_id,
           SUM(CAST(ROUND(p.payment_value * 100) AS INTEGER)) AS paid_cents
    FROM main.order_payments AS p
    GROUP BY p.order_id
),
customer_totals AS (
    SELECT c.customer_unique_id,
           MAX(o.order_purchase_timestamp) AS last_purchase,
           COUNT(*) AS order_count,
           -- One delivered order has no payment record. Its customer's spend stays NULL.
           CASE WHEN COUNT(p.paid_cents) = COUNT(*) THEN SUM(p.paid_cents) END AS spend_cents
    FROM main.orders AS o
    JOIN main.customers AS c ON c.customer_id = o.customer_id
    LEFT JOIN payments_by_order AS p ON p.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_unique_id
),
customer_rfm AS (
    SELECT customer_unique_id, order_count, spend_cents,
           -- Elapsed days from this customer's last purchase to the latest delivered-order
           -- purchase timestamp, 2018-08-29 15:00:37 in this snapshot.
           JULIANDAY(MAX(last_purchase) OVER ()) - JULIANDAY(last_purchase) AS recency_days
    FROM customer_totals
),
scores AS (
    -- Each percentile is the fraction of customers with an equal or better value.
    -- Ties share a score, so all 90,557 one-order customers receive F = 5.
    SELECT customer_unique_id, order_count, spend_cents,
           CEIL(5 * CUME_DIST() OVER (ORDER BY recency_days)) AS r,
           CEIL(5 * CUME_DIST() OVER (ORDER BY order_count DESC)) AS f,
           CASE WHEN spend_cents IS NOT NULL THEN
               CEIL(5 * CUME_DIST() OVER (
                   PARTITION BY (spend_cents IS NULL) ORDER BY spend_cents DESC
               ))
           END AS m
    FROM customer_rfm
),
segmented AS (
    SELECT customer_unique_id, order_count, spend_cents,
           CASE
               WHEN r = 1 AND f + m BETWEEN 1 AND 4 THEN 'Champions'
               WHEN r IN (4, 5) AND f + m BETWEEN 1 AND 2 THEN 'Can''t Lose Them'
               WHEN r IN (4, 5) AND f + m BETWEEN 3 AND 6 THEN 'Hibernating'
               WHEN r IN (4, 5) AND f + m BETWEEN 7 AND 10 THEN 'Lost'
               WHEN r IN (2, 3) AND f + m BETWEEN 1 AND 4 THEN 'Loyal Customers'
               WHEN r = 3 AND f + m BETWEEN 5 AND 6 THEN 'Needs Attention'
               WHEN r = 1 AND f + m BETWEEN 7 AND 8 THEN 'Recent Users'
               WHEN (r = 1 AND f + m BETWEEN 5 AND 6)
                 OR (r = 2 AND f + m BETWEEN 5 AND 8) THEN 'Potential Loyalists'
               WHEN r = 1 AND f + m BETWEEN 9 AND 10 THEN 'Price Sensitive'
               WHEN r = 2 AND f + m BETWEEN 9 AND 10 THEN 'Promising'
               WHEN r = 3 AND f + m BETWEEN 7 AND 10 THEN 'About to Sleep'
           END AS rfm_segment
    FROM scores
),
averages AS (
    SELECT rfm_segment, customer_unique_id,
           spend_cents / (100.0 * order_count) AS customer_avg_sales_per_order,
           SUM(spend_cents) OVER segment / (100.0 * SUM(order_count) OVER segment)
               AS segment_avg_sales_per_order
    FROM segmented
    WINDOW segment AS (PARTITION BY rfm_segment)
)
-- Needs Attention averaged 405.27 per order, followed by Hibernating at 399.07.
-- One-order customers in Needs Attention all scored M = 1. Price Sensitive averaged
-- 54.51 per order, the lowest of the 11 segments.
SELECT rfm_segment, customer_unique_id, customer_avg_sales_per_order,
       segment_avg_sales_per_order,
       segment_avg_sales_per_order - MIN(segment_avg_sales_per_order) OVER ()
           AS difference_from_lowest_segment
FROM averages
ORDER BY segment_avg_sales_per_order DESC, rfm_segment, customer_unique_id
LIMIT -1
