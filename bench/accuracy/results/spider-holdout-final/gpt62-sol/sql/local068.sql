WITH years(year) AS (
    VALUES (2021), (2022), (2023)
), months(month) AS (
    VALUES (4), (5), (6)
), monthly_counts AS (
    SELECT y.year, m.month, COUNT(c.insert_date) AS monthly_total
    FROM years AS y
    CROSS JOIN months AS m
    LEFT JOIN cities AS c
        ON c.insert_date >= printf('%04d-%02d-01', y.year, m.month)
       AND c.insert_date <  printf('%04d-%02d-01', y.year, m.month + 1)
    GROUP BY y.year, m.month
), running_totals AS (
    SELECT year, month, monthly_total,
           SUM(monthly_total) OVER (
               PARTITION BY month ORDER BY year
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS running_total
    FROM monthly_counts
), prior_year AS (
    SELECT year, month, monthly_total, running_total,
           LAG(monthly_total) OVER (PARTITION BY month ORDER BY year) AS prev_monthly_total,
           LAG(running_total) OVER (PARTITION BY month ORDER BY year) AS prev_running_total
    FROM running_totals
)
SELECT year, month, monthly_total, running_total,
       ROUND(100.0 * (monthly_total - prev_monthly_total) /
             NULLIF(prev_monthly_total, 0), 2) AS monthly_yoy_growth_pct,
       ROUND(100.0 * (running_total - prev_running_total) /
             NULLIF(prev_running_total, 0), 2) AS running_yoy_growth_pct
FROM prior_year
WHERE year IN (2022, 2023)
ORDER BY year, month;
