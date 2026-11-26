WITH period_sales AS (
    SELECT region, platform, age_band, demographic, customer_type, sales,
           week_date < '2020-06-15' AS is_before
    FROM main.cleaned_weekly_sales
    WHERE week_date >= date('2020-06-15', '-84 days')
      AND week_date < date('2020-06-15', '+84 days')
),
attribute_sales(attribute_type, attribute_value, is_before, sales) AS (
    SELECT 'region', region, is_before, sales FROM period_sales
    UNION ALL
    SELECT 'platform', platform, is_before, sales FROM period_sales
    UNION ALL
    SELECT 'age_band', age_band, is_before, sales FROM period_sales
    UNION ALL
    SELECT 'demographic', demographic, is_before, sales FROM period_sales
    UNION ALL
    SELECT 'customer_type', customer_type, is_before, sales FROM period_sales
),
value_totals AS (
    SELECT attribute_type, attribute_value,
           SUM(CASE WHEN is_before THEN sales ELSE 0 END) AS sales_before,
           SUM(CASE WHEN is_before THEN 0 ELSE sales END) AS sales_after
    FROM attribute_sales
    GROUP BY attribute_type, attribute_value
)
SELECT attribute_type,
       AVG(100.0 * (sales_after - sales_before) / NULLIF(sales_before, 0))
           AS average_percentage_change
FROM value_totals
GROUP BY attribute_type
ORDER BY average_percentage_change ASC, attribute_type
LIMIT 1
