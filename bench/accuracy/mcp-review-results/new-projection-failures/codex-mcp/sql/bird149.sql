WITH customer_totals AS (
    SELECT c.Segment, c.CustomerID,
           SUM(CAST(ROUND(y.Consumption * 100) AS INTEGER)) AS total_cents
    FROM main.customers AS c
    JOIN main.yearmonth AS y ON y.CustomerID = c.CustomerID
    WHERE c.Currency = 'CZK'
      AND c.Segment IN ('SME', 'LAM', 'KAM')
      AND y.Date >= '201301' AND y.Date < '201401'
    GROUP BY c.Segment, c.CustomerID
    HAVING COUNT(y.Consumption) > 0
),
ranked AS (
    SELECT Segment, CustomerID, total_cents,
           ROW_NUMBER() OVER (
               PARTITION BY Segment ORDER BY total_cents, CustomerID
           ) AS position
    FROM customer_totals
),
winners AS (
    SELECT Segment, CustomerID, total_cents
    FROM ranked
    WHERE position = 1
)
SELECT sme.CustomerID AS sme_customer_id,
       lam.CustomerID AS lam_customer_id,
       kam.CustomerID AS kam_customer_id,
       ROUND((sme.total_cents - lam.total_cents) / 100.0, 2) AS sme_minus_lam,
       ROUND((lam.total_cents - kam.total_cents) / 100.0, 2) AS lam_minus_kam,
       ROUND((kam.total_cents - sme.total_cents) / 100.0, 2) AS kam_minus_sme
FROM winners AS sme
CROSS JOIN winners AS lam
CROSS JOIN winners AS kam
WHERE sme.Segment = 'SME' AND lam.Segment = 'LAM' AND kam.Segment = 'KAM'
LIMIT 1
