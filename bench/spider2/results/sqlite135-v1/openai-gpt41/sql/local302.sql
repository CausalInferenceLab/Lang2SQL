WITH sales_periods AS (
  SELECT
    region,
    platform,
    age_band,
    demographic,
    customer_type,
    CASE
      WHEN week_date < '2020-06-15' AND week_date >= date('2020-06-15', '-12 week') THEN 'pre'
      WHEN week_date >= '2020-06-15' AND week_date < date('2020-06-15', '+12 week') THEN 'post'
      ELSE NULL
    END AS period,
    sales
  FROM cleaned_weekly_sales
  WHERE
    week_date >= date('2020-06-15', '-12 week')
    AND week_date < date('2020-06-15', '+12 week')
),
agg_sales AS (
  SELECT
    region,
    platform,
    age_band,
    demographic,
    customer_type,
    period,
    SUM(sales) AS total_sales
  FROM sales_periods
  WHERE period IS NOT NULL
  GROUP BY region, platform, age_band, demographic, customer_type, period
),
pct_change AS (
  SELECT
    region,
    platform,
    age_band,
    demographic,
    customer_type,
    CASE
      WHEN pre_sales > 0 THEN 100.0 * (post_sales - pre_sales) / pre_sales
      ELSE NULL
    END AS pct_change
  FROM (
    SELECT
      a.region, a.platform, a.age_band, a.demographic, a.customer_type,
      a.total_sales AS pre_sales,
      b.total_sales AS post_sales
    FROM agg_sales a
    JOIN agg_sales b
      ON a.region = b.region
      AND a.platform = b.platform
      AND a.age_band = b.age_band
      AND a.demographic = b.demographic
      AND a.customer_type = b.customer_type
    WHERE a.period = 'pre' AND b.period = 'post'
  )
),
attr_type_avg AS (
  SELECT 'region' AS attr_type, AVG(pct_change) AS avg_pct_change FROM pct_change GROUP BY region
  UNION ALL
  SELECT 'platform', AVG(pct_change) FROM pct_change GROUP BY platform
  UNION ALL
  SELECT 'age_band', AVG(pct_change) FROM pct_change GROUP BY age_band
  UNION ALL
  SELECT 'demographic', AVG(pct_change) FROM pct_change GROUP BY demographic
  UNION ALL
  SELECT 'customer_type', AVG(pct_change) FROM pct_change GROUP BY customer_type
),
final_avg AS (
  SELECT attr_type, AVG(avg_pct_change) AS avg_pct_change
  FROM attr_type_avg
  GROUP BY attr_type
)
SELECT
  attr_type,
  avg_pct_change
FROM final_avg
ORDER BY avg_pct_change ASC
LIMIT 1;
