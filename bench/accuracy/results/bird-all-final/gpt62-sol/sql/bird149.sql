WITH customer_totals AS (
    SELECT c.Segment, c.CustomerID, SUM(y.Consumption) AS total_consumption
    FROM customers AS c
    JOIN yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE c.Currency = 'CZK'
      AND c.Segment IN ('SME', 'LAM', 'KAM')
      AND substr(y.Date, 1, 4) = '2013'
    GROUP BY c.Segment, c.CustomerID
), ranked AS (
    SELECT Segment, total_consumption,
           ROW_NUMBER() OVER (
               PARTITION BY Segment
               ORDER BY total_consumption ASC NULLS LAST, CustomerID ASC
           ) AS segment_rank
    FROM customer_totals
), lowest_totals AS (
    SELECT MAX(CASE WHEN Segment = 'SME' THEN total_consumption END) AS sme,
           MAX(CASE WHEN Segment = 'LAM' THEN total_consumption END) AS lam,
           MAX(CASE WHEN Segment = 'KAM' THEN total_consumption END) AS kam
    FROM ranked
    WHERE segment_rank = 1
)
SELECT ROUND(sme - lam, 2) AS sme_minus_lam,
       ROUND(lam - kam, 2) AS lam_minus_kam,
       ROUND(kam - sme, 2) AS kam_minus_sme
FROM lowest_totals;
