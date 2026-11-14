WITH monthly_finished_orders AS (
  SELECT 
    o.store_id AS hub_id,
    strftime('%Y-%m', o.order_moment_finished) AS order_month,
    COUNT(*) AS finished_order_count
  FROM orders o
  WHERE o.order_status = 'finished'
    AND strftime('%Y-%m', o.order_moment_finished) IN ('2023-02', '2023-03')
  GROUP BY o.store_id, order_month
)
SELECT 
  fhub.hub_id,
  hb.hub_name,
  fhub.finished_order_count AS finished_orders_february,
  mhub.finished_order_count AS finished_orders_march,
  ((mhub.finished_order_count - fhub.finished_order_count) * 1.0 / fhub.finished_order_count) * 100 AS percent_increase
FROM monthly_finished_orders fhub
JOIN monthly_finished_orders mhub ON fhub.hub_id = mhub.hub_id
JOIN hubs hb ON hb.hub_id = fhub.hub_id
WHERE fhub.order_month = '2023-02'
  AND mhub.order_month = '2023-03'
  AND ((mhub.finished_order_count - fhub.finished_order_count) * 1.0 / fhub.finished_order_count) > 0.2
ORDER BY percent_increase DESC;
