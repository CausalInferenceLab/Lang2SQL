WITH daily_deliveries AS (
  SELECT
    driver_id,
    DATE(delivery_date) AS delivery_day,
    COUNT(*) AS deliveries_count
  FROM deliveries
  GROUP BY driver_id, delivery_day
),
avg_daily_deliveries AS (
  SELECT
    driver_id,
    AVG(deliveries_count) AS avg_daily_deliveries
  FROM daily_deliveries
  GROUP BY driver_id
)
SELECT
  driver_id,
  avg_daily_deliveries
FROM avg_daily_deliveries
ORDER BY avg_daily_deliveries DESC
LIMIT 5;
