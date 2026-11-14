WITH daily_deliveries AS (
  SELECT
    driver_id,
    DATE(delivery_time) AS delivery_date,
    COUNT(*) AS deliveries_per_day
  FROM deliveries
  GROUP BY driver_id, delivery_date
),
avg_daily_deliveries AS (
  SELECT
    driver_id,
    AVG(deliveries_per_day) AS avg_daily_deliveries
  FROM daily_deliveries
  GROUP BY driver_id
)
SELECT
  d.driver_id,
  dr.name,
  avg_daily_deliveries
FROM avg_daily_deliveries d
JOIN drivers dr ON d.driver_id = dr.id
ORDER BY avg_daily_deliveries DESC
LIMIT 5;
