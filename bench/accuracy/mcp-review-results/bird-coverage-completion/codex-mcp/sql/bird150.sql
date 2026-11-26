WITH segment_totals AS (
    SELECT c.Segment,
           SUM(CASE WHEN y.Date < '201301' THEN y.Consumption END) AS total_2012,
           SUM(CASE WHEN y.Date >= '201301' THEN y.Consumption END) AS total_2013
    FROM main.customers AS c
    JOIN main.yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE c.Currency = 'EUR'
      AND c.Segment IN ('SME', 'LAM', 'KAM')
      AND y.Date >= '201201'
      AND y.Date < '201401'
    GROUP BY c.Segment
),
segment_growth AS (
    SELECT Segment,
           100.0 * (total_2013 - total_2012) / NULLIF(total_2012, 0) AS growth_pct
    FROM segment_totals
),
extremes AS (
    SELECT Segment, growth_pct,
           MAX(growth_pct) OVER () AS highest_pct,
           MIN(growth_pct) OVER () AS lowest_pct
    FROM segment_growth
)
SELECT Segment, ROUND(growth_pct, 2) AS increase_percentage
FROM extremes
WHERE growth_pct = highest_pct OR growth_pct = lowest_pct
ORDER BY growth_pct DESC, Segment
LIMIT 3
