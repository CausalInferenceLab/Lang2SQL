WITH sales_window AS (
    SELECT week_date, region, platform, age_band, demographic, customer_type, sales
    FROM cleaned_weekly_sales
    WHERE week_date >= date('2020-06-15', '-84 days')
      AND week_date < date('2020-06-15', '+84 days')
),
attribute_sales AS (
    SELECT 'region' AS attribute_type, region AS attribute_value, week_date, sales
    FROM sales_window
    UNION ALL
    SELECT 'platform', platform, week_date, sales FROM sales_window
    UNION ALL
    SELECT 'age_band', age_band, week_date, sales FROM sales_window
    UNION ALL
    SELECT 'demographic', demographic, week_date, sales FROM sales_window
    UNION ALL
    SELECT 'customer_type', customer_type, week_date, sales FROM sales_window
),
value_totals AS (
    SELECT attribute_type,
           attribute_value,
           SUM(CASE WHEN week_date < '2020-06-15' THEN sales ELSE 0 END) AS sales_before,
           SUM(CASE WHEN week_date >= '2020-06-15' THEN sales ELSE 0 END) AS sales_after
    FROM attribute_sales
    GROUP BY attribute_type, attribute_value
),
type_averages AS (
    SELECT attribute_type,
           AVG(100.0 * (sales_after - sales_before) / NULLIF(sales_before, 0)) AS avg_percentage_change
    FROM value_totals
    GROUP BY attribute_type
)
SELECT attribute_type, avg_percentage_change
FROM type_averages
WHERE avg_percentage_change = (SELECT MIN(avg_percentage_change) FROM type_averages)
  AND avg_percentage_change < 0
ORDER BY attribute_type;
