WITH sales_data AS (
  SELECT
    CAST(strftime('%Y', week_date) AS INTEGER) AS year,
    week_date,
    sales
  FROM weekly_sales
  WHERE year IN (2018, 2019, 2020)
    AND (week_date BETWEEN date(year || '-05-18') AND date(year || '-07-13'))
),
segments AS (
  SELECT
    year,
    SUM(sales) AS total_sales,
    CASE
      WHEN week_date < date(year || '-06-15') THEN 'before'
      WHEN week_date >= date(year || '-06-15') THEN 'after'
    END AS period
  FROM sales_data
  GROUP BY year, period
),
pivoted AS (
  SELECT
    year,
    MAX(CASE WHEN period = 'before' THEN total_sales ELSE 0 END) AS sales_before,
    MAX(CASE WHEN period = 'after' THEN total_sales ELSE 0 END) AS sales_after
  FROM segments
  GROUP BY year
)
SELECT
  year,
  sales_before,
  sales_after,
  ROUND(((sales_after - sales_before) * 100.0) / sales_before, 2) AS pct_change
FROM pivoted
ORDER BY year;
