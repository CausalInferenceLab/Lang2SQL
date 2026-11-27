WITH payment_totals AS (
    -- Sum all payment rows before joining, so each delivered order counts once.
    SELECT order_id, SUM(payment_value) AS order_spend
    FROM order_payments
    GROUP BY order_id
), customer_totals AS (
    -- Use all available delivered orders. Keep orders with no recorded payment
    -- in the denominator and flag them; they add nothing to recorded spend.
    SELECT c.customer_unique_id,
           MAX(o.order_purchase_timestamp) AS last_purchase_timestamp,
           COUNT(*) AS order_count,
           ROUND(SUM(COALESCE(p.order_spend, 0.0)), 2) AS total_spend,
           SUM(p.order_spend IS NULL) AS orders_without_payment
    FROM orders AS o
    JOIN customers AS c ON c.customer_id = o.customer_id
    LEFT JOIN payment_totals AS p ON p.order_id = o.order_id
    WHERE o.order_status = 'delivered'
      AND c.customer_unique_id IS NOT NULL
    GROUP BY c.customer_unique_id
), reference_date AS (
    SELECT MAX(last_purchase_timestamp) AS as_of_timestamp
    FROM customer_totals
), customer_rfm AS (
    -- Recency is elapsed days from the customer's latest delivered purchase
    -- to the latest delivered purchase timestamp in the analysis population.
    SELECT c.*, d.as_of_timestamp,
           JULIANDAY(d.as_of_timestamp) - JULIANDAY(c.last_purchase_timestamp) AS recency_days
    FROM customer_totals AS c
    CROSS JOIN reference_date AS d
), percentiles AS (
    -- Rank best to worst. CUME_DIST keeps ties together at the upper end
    -- of their percentile range; not all five scores need to be populated.
    SELECT *,
           CUME_DIST() OVER (ORDER BY recency_days ASC) AS r_pct,
           CUME_DIST() OVER (ORDER BY order_count DESC) AS f_pct,
           CUME_DIST() OVER (ORDER BY total_spend DESC) AS m_pct
    FROM customer_rfm
), scores AS (
    -- 1 = best 20%; then 2, 3, 4, and 5 at successive 20-point cutoffs.
    SELECT *,
           1 + (r_pct > 0.2) + (r_pct > 0.4) + (r_pct > 0.6) + (r_pct > 0.8) AS r_score,
           1 + (f_pct > 0.2) + (f_pct > 0.4) + (f_pct > 0.6) + (f_pct > 0.8) AS f_score,
           1 + (m_pct > 0.2) + (m_pct > 0.4) + (m_pct > 0.6) + (m_pct > 0.8) AS m_score
    FROM percentiles
), cells AS (
    SELECT *, r_score || f_score || m_score AS rfm_cell,
           f_score + m_score AS fm_score
    FROM scores
), segmented AS (
    SELECT *, CASE
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
        ELSE 'Unclassified'
    END AS rfm_segment
    FROM cells
), segment_totals AS (
    SELECT rfm_segment,
           COUNT(*) AS segment_customer_count,
           SUM(order_count) AS segment_order_count,
           SUM(total_spend) AS segment_total_spend,
           SUM(orders_without_payment) AS segment_orders_without_payment
    FROM segmented
    GROUP BY rfm_segment
), segment_comparison AS (
    -- Segment and overall averages are order-weighted: spend / order count.
    SELECT *,
           segment_total_spend / NULLIF(segment_order_count, 0) AS segment_avg,
           SUM(segment_total_spend) OVER () / NULLIF(SUM(segment_order_count) OVER (), 0) AS overall_avg,
           DENSE_RANK() OVER (
               ORDER BY segment_total_spend / NULLIF(segment_order_count, 0) DESC
           ) AS segment_rank
    FROM segment_totals
)
SELECT c.customer_unique_id,
       c.rfm_segment, c.rfm_cell, c.r_score, c.f_score, c.m_score,
       c.last_purchase_timestamp, c.as_of_timestamp,
       ROUND(c.recency_days, 2) AS recency_days,
       c.order_count, c.total_spend, c.orders_without_payment,
       ROUND(c.total_spend / NULLIF(c.order_count, 0), 2) AS customer_avg_sales_per_order,
       s.segment_customer_count, s.segment_order_count,
       ROUND(s.segment_total_spend, 2) AS segment_total_spend,
       s.segment_orders_without_payment,
       ROUND(s.segment_avg, 2) AS segment_avg_sales_per_order,
       ROUND(s.overall_avg, 2) AS overall_avg_sales_per_order,
       ROUND(s.segment_avg - s.overall_avg, 2) AS segment_avg_difference_vs_overall,
       s.segment_rank AS segment_rank_by_avg_sales_per_order
FROM segmented AS c
JOIN segment_comparison AS s ON s.rfm_segment = c.rfm_segment
ORDER BY s.segment_rank, c.rfm_segment, c.customer_unique_id;
