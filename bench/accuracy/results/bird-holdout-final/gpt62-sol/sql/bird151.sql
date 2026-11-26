WITH annual_totals AS (
    SELECT
        SUM(CASE WHEN substr(ym.Date, 1, 4) = '2012' THEN ym.Consumption END) AS total_2012,
        SUM(CASE WHEN substr(ym.Date, 1, 4) = '2013' THEN ym.Consumption END) AS total_2013
    FROM yearmonth AS ym
    WHERE ym.CustomerID IN (
        SELECT t.CustomerID
        FROM transactions_1k AS t
        WHERE t.Date = '2012-08-25' AND t.Price = 634.8
    )
      AND substr(ym.Date, 1, 4) IN ('2012', '2013')
)
SELECT ROUND((total_2013 - total_2012) / NULLIF(total_2012, 0), 4) AS relative_change
FROM annual_totals;
