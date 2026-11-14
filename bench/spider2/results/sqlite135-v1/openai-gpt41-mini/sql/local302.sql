WITH sales_periods AS (
  SELECT
    week_date,
    region,
    platform,
    segment,
    customer_type,
    sales
  FROM weekly_sales
  WHERE week_date BETWEEN date('2020-03-23') AND date('2020-09-07')
),
before_after_sales AS (
  SELECT
    region,
    platform,
    segment,
    customer_type,
    CASE WHEN week_date < date('2020-06-15') THEN 'before' ELSE 'after' END AS period,
    SUM(sales) AS total_sales
  FROM sales_periods
  GROUP BY region, platform, segment, customer_type, period
),
pct_change AS (
  SELECT
    before_sales.region AS region,
    before_sales.platform AS platform,
    before_sales.segment AS segment,
    before_sales.customer_type AS customer_type,
    CASE 
      WHEN before_sales.total_sales = 0 THEN NULL
      ELSE ((after_sales.total_sales * 1.0 / before_sales.total_sales) - 1) * 100 
    END AS pct_change
  FROM
    (SELECT * FROM before_after_sales WHERE period = 'before') AS before_sales
    JOIN (SELECT * FROM before_after_sales WHERE period = 'after') AS after_sales
    ON before_sales.region = after_sales.region
    AND before_sales.platform = after_sales.platform
    AND before_sales.segment = after_sales.segment
    AND before_sales.customer_type = after_sales.customer_type
),
avg_pct_change_by_region AS (
  SELECT region, AVG(pct_change) AS avg_pct_change FROM pct_change WHERE pct_change IS NOT NULL GROUP BY region
),
avg_pct_change_by_platform AS (
  SELECT platform, AVG(pct_change) AS avg_pct_change FROM pct_change WHERE pct_change IS NOT NULL GROUP BY platform
),
avg_pct_change_by_segment AS (
  SELECT segment, AVG(pct_change) AS avg_pct_change FROM pct_change WHERE pct_change IS NOT NULL GROUP BY segment
),
avg_pct_change_by_customer_type AS (
  SELECT customer_type, AVG(pct_change) AS avg_pct_change FROM pct_change WHERE pct_change IS NOT NULL GROUP BY customer_type
),
avg_pct_change_summary AS (
  SELECT 'region' AS attribute_type, AVG(avg_pct_change) AS avg_pct_change FROM avg_pct_change_by_region
  UNION ALL
  SELECT 'platform' AS attribute_type, AVG(avg_pct_change) AS avg_pct_change FROM avg_pct_change_by_platform
  UNION ALL
  SELECT 'segment' AS attribute_type, AVG(avg_pct_change) AS avg_pct_change FROM avg_pct_change_by_segment
  UNION ALL
  SELECT 'customer_type' AS attribute_type, AVG(avg_pct_change) AS avg_pct_change FROM avg_pct_change_by_customer_type
),
max_negative_impact AS (
  SELECT attribute_type, avg_pct_change
  FROM avg_pct_change_summary
  ORDER BY avg_pct_change ASC
  LIMIT 1
)
SELECT * FROM avg_pct_change_summary
UNION ALL
SELECT * FROM max_negative_impact;
