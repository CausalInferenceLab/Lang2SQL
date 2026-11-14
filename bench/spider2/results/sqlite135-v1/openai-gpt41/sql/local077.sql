WITH monthly_interest_composition AS (
    SELECT
        strftime('%Y-%m', date) AS month,
        interest,
        AVG(CAST(composition AS FLOAT) / NULLIF(index_value, 0)) AS avg_composition
    FROM interest_metrics
    WHERE date >= '2018-09-01' AND date <= '2019-08-31'
    GROUP BY month, interest
),
monthly_max_interest AS (
    SELECT
        month,
        interest,
        avg_composition AS max_index_composition
    FROM (
        SELECT
            month,
            interest,
            avg_composition,
            ROW_NUMBER() OVER (PARTITION BY month ORDER BY avg_composition DESC) AS rn
        FROM monthly_interest_composition
    )
    WHERE rn = 1
),
rolling_max AS (
    SELECT
        m1.month,
        m1.interest,
        m1.max_index_composition,
        -- 3-month rolling average (current + prev1 + prev2) / 3
        (
            m1.max_index_composition
            + IFNULL((
                SELECT m2.max_index_composition
                FROM monthly_max_interest m2
                WHERE m2.month = strftime('%Y-%m', date(m1.month || '-01', '-1 month'))
            ), 0)
            + IFNULL((
                SELECT m3.max_index_composition
                FROM monthly_max_interest m3
                WHERE m3.month = strftime('%Y-%m', date(m1.month || '-01', '-2 month'))
            ), 0)
        ) / (
            1
            + (SELECT COUNT(*) FROM monthly_max_interest m2 WHERE m2.month = strftime('%Y-%m', date(m1.month || '-01', '-1 month')))
            + (SELECT COUNT(*) FROM monthly_max_interest m3 WHERE m3.month = strftime('%Y-%m', date(m1.month || '-01', '-2 month')))
        ) AS rolling_3mo_avg,
        -- Previous month
        (
            SELECT m2.interest
            FROM monthly_max_interest m2
            WHERE m2.month = strftime('%Y-%m', date(m1.month || '-01', '-1 month'))
        ) AS prev_month_interest,
        (
            SELECT m2.max_index_composition
            FROM monthly_max_interest m2
            WHERE m2.month = strftime('%Y-%m', date(m1.month || '-01', '-1 month'))
        ) AS prev_month_max_index_composition,
        -- Two months ago
        (
            SELECT m3.interest
            FROM monthly_max_interest m3
            WHERE m3.month = strftime('%Y-%m', date(m1.month || '-01', '-2 month'))
        ) AS prev2_month_interest,
        (
            SELECT m3.max_index_composition
            FROM monthly_max_interest m3
            WHERE m3.month = strftime('%Y-%m', date(m1.month || '-01', '-2 month'))
        ) AS prev2_month_max_index_composition
    FROM monthly_max_interest m1
    WHERE m1.month >= '2018-09' AND m1.month <= '2019-08'
)
SELECT
    month,
    interest AS max_interest_this_month,
    max_index_composition,
    ROUND(rolling_3mo_avg, 6) AS rolling_3month_avg_max_index_composition,
    prev_month_interest,
    prev_month_max_index_composition,
    prev2_month_interest,
    prev2_month_max_index_composition
FROM rolling_max
ORDER BY month;
