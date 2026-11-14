WITH june_dates AS (
  SELECT 
    strftime('%Y', week_date) AS year,
    week_date,
    ABS(julianday(week_date) - julianday(strftime('%Y', week_date) || '-06-15')) AS diff_from_june15
  FROM weekly_sales
  WHERE strftime('%Y', week_date) IN ('2018', '2019', '2020')
),
periods AS (
  SELECT 
    ws.*, 
    jd.year,
    CASE 
      WHEN ws.week_date < jd2.june15 THEN 'before'
      ELSE 'after' 
    END AS period
  FROM weekly_sales ws
  JOIN (
    SELECT year, MIN(date) as june15
    FROM (
      SELECT strftime('%Y', week_date) as year, strftime('%Y', week_date) || '-06-15' as date 
      FROM weekly_sales 
      WHERE strftime('%Y', week_date) IN ('2018', '2019', '2020')
      GROUP BY year
    )
    GROUP BY year
  ) jd2 ON strftime('%Y', ws.week_date) = jd2.year
  JOIN june_dates jd ON ws.week_date = jd.week_date AND jd.year = jd2.year
  WHERE jd.diff_from_june15 <= 27 -- within 4 weeks (4*7-1)
),
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY year, period ORDER BY ABS(julianday(week_date) - julianday(year || '-06-15'))) as rn
  FROM periods
)
SELECT 
  year,
  ROUND(SUM(CASE WHEN period = 'before' THEN sales END)*1.0, 2) AS sales_before,
  ROUND(SUM(CASE WHEN period = 'after' THEN sales END)*1.0, 2) AS sales_after,
  ROUND((SUM(CASE WHEN period = 'after' THEN sales END) - SUM(CASE WHEN period = 'before' THEN sales END)) * 100.0 / NULLIF(SUM(CASE WHEN period = 'before' THEN sales END), 0), 2) AS percent_change
FROM ranked
WHERE rn <= 4
GROUP BY year
ORDER BY year
