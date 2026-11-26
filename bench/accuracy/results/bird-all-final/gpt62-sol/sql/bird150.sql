WITH annual_totals AS (
    SELECT c.Segment,
           SUM(CASE WHEN y.Date >= '201201' AND y.Date < '201301' THEN y.Consumption END) AS total_2012,
           SUM(CASE WHEN y.Date >= '201301' AND y.Date < '201401' THEN y.Consumption END) AS total_2013
    FROM customers AS c
    JOIN yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE c.Currency = 'EUR'
      AND c.Segment IN ('SME', 'LAM', 'KAM')
      AND y.Date >= '201201' AND y.Date < '201401'
    GROUP BY c.Segment
), increases AS (
    SELECT Segment,
           100.0 * (total_2013 - total_2012) / NULLIF(total_2012, 0) AS increase_pct
    FROM annual_totals
), extremes AS (
    SELECT MIN(increase_pct) AS lowest, MAX(increase_pct) AS highest
    FROM increases
)
SELECT i.Segment, ROUND(i.increase_pct, 2) AS increase_percentage
FROM increases AS i
CROSS JOIN extremes AS e
WHERE i.increase_pct IN (e.lowest, e.highest)
ORDER BY i.increase_pct DESC, i.Segment;
