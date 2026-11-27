WITH periods(year, cutoff) AS (
    VALUES (2018, '2018-06-15'), (2019, '2019-06-15'), (2020, '2020-06-15')
), totals AS (
    SELECT p.year,
           SUM(CASE WHEN s.week_date < p.cutoff THEN s.sales END) AS sales_before,
           SUM(CASE WHEN s.week_date >= p.cutoff THEN s.sales END) AS sales_after
    FROM periods AS p
    LEFT JOIN cleaned_weekly_sales AS s
      ON s.week_date >= date(p.cutoff, '-28 days')
     AND s.week_date <  date(p.cutoff, '+28 days')
    GROUP BY p.year
)
SELECT year,
       ROUND(100.0 * (sales_after - sales_before) / NULLIF(sales_before, 0), 2) AS sales_pct_change
FROM totals
ORDER BY year;
