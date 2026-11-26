WITH weekly AS (
    SELECT region, platform, age_band, demographic, customer_type, sales,
           week_date >= '2020-06-15' AS is_after
    FROM main.cleaned_weekly_sales
    WHERE week_date >= date('2020-06-15', '-84 days')
      AND week_date < date('2020-06-15', '+84 days')
),
attributes(attribute_type, attribute_value, is_after, sales) AS (
    SELECT 'region', region, is_after, sales FROM weekly
    UNION ALL
    SELECT 'platform', platform, is_after, sales FROM weekly
    UNION ALL
    SELECT 'age_band', age_band, is_after, sales FROM weekly
    UNION ALL
    SELECT 'demographic', demographic, is_after, sales FROM weekly
    UNION ALL
    SELECT 'customer_type', customer_type, is_after, sales FROM weekly
),
value_totals AS (
    SELECT attribute_type, attribute_value,
           SUM(CASE WHEN is_after = 0 THEN sales ELSE 0 END) AS sales_before,
           SUM(CASE WHEN is_after = 1 THEN sales ELSE 0 END) AS sales_after
    FROM attributes
    GROUP BY attribute_type, attribute_value
),
type_averages AS (
    SELECT attribute_type,
           AVG(100.0 * (sales_after - sales_before) / NULLIF(sales_before, 0))
               AS average_percentage_change
    FROM value_totals
    GROUP BY attribute_type
)
SELECT attribute_type, average_percentage_change
FROM type_averages
WHERE average_percentage_change < 0
ORDER BY average_percentage_change, attribute_type
LIMIT 1
