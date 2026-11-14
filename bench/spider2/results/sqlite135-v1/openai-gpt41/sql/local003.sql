WITH delivered_orders AS (
  SELECT o.order_id, o.customer_id, o.order_purchase_timestamp
  FROM orders o
  WHERE o.order_status = 'delivered'
),
customer_spend_orders AS (
  SELECT c.customer_unique_id,
         o.order_id,
         MAX(o.order_purchase_timestamp) AS order_purchase_timestamp,
         SUM(oi.price + oi.freight_value) AS order_total
    FROM customers c
    JOIN delivered_orders o ON c.customer_id = o.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
   GROUP BY c.customer_unique_id, o.order_id
),
rfm_raw AS (
  SELECT 
    customer_unique_id,
    MAX(order_purchase_timestamp) AS last_purchase,
    COUNT(order_id) AS freq,
    SUM(order_total) AS monetary
  FROM customer_spend_orders
  GROUP BY customer_unique_id
),
recency_ranked AS (
  SELECT *, NTILE(5) OVER (ORDER BY last_purchase DESC) AS r
  FROM rfm_raw
),
frequency_ranked AS (
  SELECT *, NTILE(5) OVER (ORDER BY freq DESC) AS f
  FROM recency_ranked
),
monetary_ranked AS (
  SELECT *, NTILE(5) OVER (ORDER BY monetary DESC) AS m
  FROM frequency_ranked
),
rfm_segmented AS (
  SELECT customer_unique_id,
         r, f, m,
         (f + m) AS fm_sum,
         CASE 
           WHEN r = 1 AND (f + m) BETWEEN 1 AND 4 THEN 'Champions'
           WHEN r IN (4,5) AND (f + m) BETWEEN 1 AND 2 THEN "Can't Lose Them"
           WHEN r IN (4,5) AND (f + m) BETWEEN 3 AND 6 THEN 'Hibernating'
           WHEN r IN (4,5) AND (f + m) BETWEEN 7 AND 10 THEN 'Lost'
           WHEN r IN (2,3) AND (f + m) BETWEEN 1 AND 4 THEN 'Loyal Customers'
           WHEN r = 3 AND (f + m) BETWEEN 5 AND 6 THEN 'Needs Attention'
           WHEN r = 1 AND (f + m) BETWEEN 7 AND 8 THEN 'Recent Users'
           WHEN (r = 1 AND (f + m) BETWEEN 5 AND 6) OR (r = 2 AND (f + m) BETWEEN 5 AND 8) THEN 'Potential Loyalists'
           WHEN r = 1 AND (f + m) BETWEEN 9 AND 10 THEN 'Price Sensitive'
           WHEN r = 2 AND (f + m) BETWEEN 9 AND 10 THEN 'Promising'
           WHEN r = 3 AND (f + m) BETWEEN 7 AND 10 THEN 'About to Sleep'
           ELSE 'Other'
         END AS rfm_segment
  FROM monetary_ranked
),
avg_sales_per_order AS (
  SELECT c.customer_unique_id, SUM(oi.price + oi.freight_value) AS total_sales, COUNT(DISTINCT o.order_id) AS total_orders
  FROM customers c
  JOIN delivered_orders o ON c.customer_id = o.customer_id
  JOIN order_items oi ON o.order_id = oi.order_id
  GROUP BY c.customer_unique_id
),
rfm_result AS (
  SELECT s.rfm_segment, s.customer_unique_id, a.total_sales, a.total_orders, 
         CASE WHEN a.total_orders > 0 THEN a.total_sales * 1.0 / a.total_orders ELSE 0 END AS avg_sales_per_order
  FROM rfm_segmented s
  JOIN avg_sales_per_order a ON a.customer_unique_id = s.customer_unique_id
)
SELECT rfm_segment, 
       COUNT(customer_unique_id) AS num_customers,
       AVG(avg_sales_per_order) AS avg_order_sales_segment
FROM rfm_result
GROUP BY rfm_segment
ORDER BY avg_order_sales_segment DESC
