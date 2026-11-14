WITH quarters AS (
    SELECT 
        t.time_id, 
        t.quarter, 
        t.year 
    FROM times t 
    WHERE (t.year = 2019 AND t.quarter = 'Q4') OR (t.year = 2020 AND t.quarter = 'Q4')
),
us_cities_sales AS (
    SELECT
        s.city,
        s.product_id,
        s.time_id,
        s.promotion_id,
        s.sales_amount
    FROM sales s
    JOIN countries c ON s.country_id = c.country_id
    WHERE c.country_name = 'United States'
),
filtered_sales AS (
    SELECT
        u.city,
        qs.year,
        qs.quarter,
        u.product_id,
        u.sales_amount
    FROM us_cities_sales u
    JOIN quarters qs ON u.time_id = qs.time_id
    WHERE u.promotion_id IS NULL
),
city_totals AS (
    SELECT
        city,
        year,
        SUM(sales_amount) AS total_sales
    FROM filtered_sales
    GROUP BY city, year
),
rising_cities AS (
    SELECT
        c2019.city
    FROM 
        city_totals c2019
        JOIN city_totals c2020 ON c2019.city = c2020.city AND c2019.year = 2019 AND c2020.year = 2020
    WHERE
        c2019.total_sales > 0
        AND c2020.total_sales >= 1.2 * c2019.total_sales
),
city_quarter_product_sales AS (
    SELECT
        fs.city,
        fs.product_id,
        fs.year,
        SUM(fs.sales_amount) AS product_sales
    FROM filtered_sales fs
    WHERE fs.city IN (SELECT city FROM rising_cities)
    GROUP BY fs.city, fs.product_id, fs.year
),
total_sales_in_cities AS (
    SELECT
        fs.city,
        fs.year,
        SUM(fs.sales_amount) as total_sales
    FROM filtered_sales fs
    WHERE fs.city IN (SELECT city FROM rising_cities)
    GROUP BY fs.city, fs.year
),
overall_product_sales AS (
    SELECT
        cp.product_id,
        SUM(cp.product_sales) AS total_sales
    FROM city_quarter_product_sales cp
    GROUP BY cp.product_id
),
top_products AS (
    SELECT
        product_id
    FROM (
        SELECT
            product_id,
            total_sales,
            ROW_NUMBER() OVER (ORDER BY total_sales DESC) as rn,
            COUNT(*) OVER () AS total_count
        FROM overall_product_sales
    ) ranked
    WHERE rn <= CAST(0.2 * total_count AS INTEGER) + CASE WHEN (0.2 * total_count) - CAST(0.2 * total_count AS INTEGER) > 0 THEN 1 ELSE 0 END
),
top_sales_by_quarter AS (
    SELECT
        fs.year,
        fs.product_id,
        SUM(fs.sales_amount) AS sales
    FROM filtered_sales fs
    WHERE fs.city IN (SELECT city FROM rising_cities)
      AND fs.product_id IN (SELECT product_id FROM top_products)
    GROUP BY fs.year, fs.product_id
),
total_sales_by_quarter AS (
    SELECT
        fs.year,
        SUM(fs.sales_amount) AS total_sales
    FROM filtered_sales fs
    WHERE fs.city IN (SELECT city FROM rising_cities)
    GROUP BY fs.year
),
share_calculation AS (
    SELECT
        t.product_id,
        t.year,
        t.sales,
        ts.total_sales,
        1.0 * t.sales / ts.total_sales AS share
    FROM top_sales_by_quarter t
    JOIN total_sales_by_quarter ts ON t.year = ts.year
),
share_pivot AS (
    SELECT
        product_id,
        MAX(CASE WHEN year = 2019 THEN share END) AS share_2019,
        MAX(CASE WHEN year = 2020 THEN share END) AS share_2020
    FROM share_calculation
    GROUP BY product_id
)
SELECT
    p.product_id,
    pr.product_name,
    sp.share_2019,
    sp.share_2020,
    (sp.share_2020 - sp.share_2019) AS share_change
FROM share_pivot sp
JOIN products pr ON sp.product_id = pr.product_id
ORDER BY share_change DESC;
