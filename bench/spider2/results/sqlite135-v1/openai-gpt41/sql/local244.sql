WITH
-- Basic stats for duration in milliseconds
dur_stats AS (
  SELECT
    MIN(Milliseconds) AS min_ms,
    AVG(Milliseconds) AS avg_ms,
    MAX(Milliseconds) AS max_ms
  FROM Track
),
-- Midpoints per definition
length_midpoints AS (
  SELECT
    min_ms,
    avg_ms,
    max_ms,
    (min_ms + avg_ms) / 2.0 AS mid1,
    (avg_ms + max_ms) / 2.0 AS mid2
  FROM dur_stats
),
-- Classify each track into category
track_category AS (
  SELECT
    t.TrackId,
    t.Milliseconds,
    CASE
      WHEN t.Milliseconds >= mp.min_ms AND t.Milliseconds < mp.mid1 THEN 'Short'
      WHEN t.Milliseconds >= mp.mid1 AND t.Milliseconds < mp.mid2 THEN 'Medium'
      WHEN t.Milliseconds >= mp.mid2 AND t.Milliseconds <= mp.max_ms THEN 'Long'
      ELSE 'Unclassified'
    END AS category
  FROM Track t
  CROSS JOIN length_midpoints mp
),
-- Map InvoiceLines to track categories
il_with_cat AS (
  SELECT
    il.InvoiceLineId,
    il.UnitPrice,
    il.Quantity,
    tc.category,
    tc.Milliseconds
  FROM InvoiceLine il
  JOIN track_category tc ON il.TrackId = tc.TrackId
),
-- Calculate min/max duration (in min), total revenue per category
agg AS (
  SELECT
    category,
    MIN(Milliseconds)/60000.0 AS min_duration_min,
    MAX(Milliseconds)/60000.0 AS max_duration_min,
    SUM(UnitPrice * Quantity) AS total_revenue
  FROM il_with_cat
  WHERE category != 'Unclassified'
  GROUP BY category
)
SELECT
  category,
  ROUND(min_duration_min,2) AS min_duration_minutes,
  ROUND(max_duration_min,2) AS max_duration_minutes,
  ROUND(total_revenue,2) AS total_revenue
FROM agg
ORDER BY category
