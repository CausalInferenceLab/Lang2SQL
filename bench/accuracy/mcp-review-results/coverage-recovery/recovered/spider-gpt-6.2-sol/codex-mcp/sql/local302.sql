WITH sales_window AS (
    SELECT week_date, region, platform, age_band, demographic, customer_type, sales
    FROM main.cleaned_weekly_sales
    WHERE week_date >= '2020-03-23' AND week_date < '2020-09-07'
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
period_totals AS (
    SELECT
        attribute_type,
        attribute_value,
        SUM(CASE WHEN week_date < '2020-06-15' THEN sales ELSE 0 END) AS sales_before,
        SUM(CASE WHEN week_date >= '2020-06-15' THEN sales ELSE 0 END) AS sales_after
    FROM attribute_sales
    GROUP BY attribute_type, attribute_value
),
attribute_averages AS (
    SELECT
        attribute_type,
        AVG(100.0 * (sales_after - sales_before) / NULLIF(sales_before, 0))
            AS avg_percentage_change
    FROM period_totals
    GROUP BY attribute_type
)
SELECT
    attribute_type,
    avg_percentage_change,
    CASE
        WHEN avg_percentage_change < 0
            AND avg_percentage_change = MIN(avg_percentage_change) OVER ()
        THEN 1
        ELSE 0
    END AS is_highest_negative_impact
FROM attribute_averages
ORDER BY avg_percentage_change ASC NULLS LAST, attribute_type
LIMIT 5
