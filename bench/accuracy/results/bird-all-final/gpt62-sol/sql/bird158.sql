WITH monthly_totals AS (
    SELECT "Date" AS calendar_month, SUM(Consumption) AS total_consumption
    FROM yearmonth
    WHERE "Date" >= '201201' AND "Date" < '201401'
    GROUP BY "Date"
)
SELECT ROUND(
    MAX(CASE WHEN SUBSTR(calendar_month, 1, 4) = '2013' THEN total_consumption END)
    - MAX(CASE WHEN SUBSTR(calendar_month, 1, 4) = '2012' THEN total_consumption END),
    0
) AS peak_consumption_difference
FROM monthly_totals;
