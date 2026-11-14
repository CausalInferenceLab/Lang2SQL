WITH delivered_orders AS (
    SELECT o.order_id, o.customer_id, o.order_purchase_timestamp, op.payment_value
    FROM orders o
    JOIN order_payments op ON o.order_id = op.order_id
    WHERE o.order_status = 'delivered'
),
customer_rfm_base AS (
    SELECT
        customer_id,
        MAX(order_purchase_timestamp) AS last_purchase,
        COUNT(order_id) AS frequency,
        SUM(payment_value) AS monetary
    FROM delivered_orders
    GROUP BY customer_id
),
recency_calculation AS (
    SELECT
        customer_id,
        last_purchase,
        frequency,
        monetary,
        julianday('now') - julianday(last_purchase) AS recency_days
    FROM customer_rfm_base
),
rfm_scores AS (
    SELECT
        customer_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days ASC) AS recency_score,  -- 1 is very recent
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,    -- 1 is most frequent
        NTILE(5) OVER (ORDER BY monetary DESC) AS monetary_score     -- 1 is highest spending
    FROM recency_calculation
),
rfm_segments AS (
    SELECT
        customer_id,
        recency_score,
        frequency_score,
        monetary_score,
        (frequency_score + monetary_score) AS freq_monetary_sum,
        CASE
            WHEN recency_score = 1 AND frequency_score + monetary_score BETWEEN 1 AND 4 THEN 'Champions'
            WHEN recency_score IN (4, 5) AND frequency_score + monetary_score BETWEEN 1 AND 2 THEN 'Cant Lose Them'
            WHEN recency_score IN (4, 5) AND frequency_score + monetary_score BETWEEN 3 AND 6 THEN 'Hibernating'
            WHEN recency_score IN (4, 5) AND frequency_score + monetary_score BETWEEN 7 AND 10 THEN 'Lost'
            WHEN recency_score IN (2, 3) AND frequency_score + monetary_score BETWEEN 1 AND 4 THEN 'Loyal Customers'
            WHEN recency_score = 3 AND frequency_score + monetary_score BETWEEN 5 AND 6 THEN 'Needs Attention'
            WHEN recency_score = 1 AND frequency_score + monetary_score BETWEEN 7 AND 8 THEN 'Recent Users'
            WHEN (recency_score = 1 AND frequency_score + monetary_score BETWEEN 5 AND 6) OR 
                 (recency_score = 2 AND frequency_score + monetary_score BETWEEN 5 AND 8) THEN 'Potential Loyalists'
            WHEN recency_score = 1 AND frequency_score + monetary_score BETWEEN 9 AND 10 THEN 'Price Sensitive'
            WHEN recency_score = 2 AND frequency_score + monetary_score BETWEEN 9 AND 10 THEN 'Promising'
            WHEN recency_score = 3 AND frequency_score + monetary_score BETWEEN 7 AND 10 THEN 'About to Sleep'
            ELSE 'Others'
        END AS rfm_segment
    FROM rfm_scores
),
avg_sales_per_order AS (
    SELECT
        do.customer_id,
        SUM(do.payment_value) / COUNT(DISTINCT do.order_id) AS avg_sales
    FROM delivered_orders do
    GROUP BY do.customer_id
)

SELECT
    s.rfm_segment,
    AVG(a.avg_sales) AS avg_sales_per_order_avg,
    COUNT(DISTINCT s.customer_id) AS customers_count
FROM rfm_segments s
JOIN avg_sales_per_order a ON s.customer_id = a.customer_id
GROUP BY s.rfm_segment
ORDER BY avg_sales_per_order_avg DESC
LIMIT 1000
