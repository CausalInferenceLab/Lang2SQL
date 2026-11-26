WITH monthly_totals AS (
    SELECT
        SUBSTR(ym.Date, 1, 4) AS calendar_year,
        SUM(ym.Consumption) AS total_consumption
    FROM main.yearmonth AS ym
    WHERE ym.Date >= '201201' AND ym.Date < '201401'
    GROUP BY ym.Date
)
SELECT CAST(ROUND(
    MAX(CASE WHEN calendar_year = '2013' THEN total_consumption END)
    - MAX(CASE WHEN calendar_year = '2012' THEN total_consumption END)
) AS INTEGER) AS peak_consumption_difference
FROM monthly_totals
LIMIT 1
