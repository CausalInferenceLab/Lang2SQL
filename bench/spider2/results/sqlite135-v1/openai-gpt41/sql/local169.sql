WITH first_terms AS (
    SELECT
        l.id AS legislator_id,
        MIN(lt.start) AS first_term_start
    FROM
        legislators l
        JOIN legislators_terms lt ON l.id = lt.legislator_id
    WHERE
        lt.start >= DATE('1917-01-01')
        AND lt.start <= DATE('1999-12-31')
    GROUP BY
        l.id
),
cohort_size AS (
    SELECT COUNT(*) AS initial_count FROM first_terms
),
tracked_periods AS (
    -- 20 periods (years after first_term_start)
    SELECT 1 AS year_num UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5 UNION ALL
    SELECT 6 UNION ALL
    SELECT 7 UNION ALL
    SELECT 8 UNION ALL
    SELECT 9 UNION ALL
    SELECT 10 UNION ALL
    SELECT 11 UNION ALL
    SELECT 12 UNION ALL
    SELECT 13 UNION ALL
    SELECT 14 UNION ALL
    SELECT 15 UNION ALL
    SELECT 16 UNION ALL
    SELECT 17 UNION ALL
    SELECT 18 UNION ALL
    SELECT 19 UNION ALL
    SELECT 20
),
base AS (
    SELECT
        ft.legislator_id,
        ft.first_term_start,
        (julianday(ft.first_term_start) + (tp.year_num * 365.24)) AS year_anniv_jd,
        tp.year_num
    FROM first_terms ft
    CROSS JOIN tracked_periods tp
),
legr_terms_detail AS (
    -- Pre-compute all legislator-term ranges for efficiency
    SELECT
        legislator_id,
        start,
        COALESCE(end, DATE('2099-12-31')) AS end  -- Open terms run very long
    FROM legislators_terms
),
retention_cte AS (
    SELECT
        b.year_num,
        COUNT(DISTINCT b.legislator_id) AS retained_count
    FROM base b
    JOIN legr_terms_detail ltd
      ON b.legislator_id = ltd.legislator_id
      AND DATE(b.year_anniv_jd) BETWEEN ltd.start AND ltd.end
    GROUP BY b.year_num
)
SELECT
    tp.year_num,
    COALESCE(r.retained_count, 0) AS retained_count,
    cs.initial_count,
    ROUND(COALESCE(r.retained_count, 0) * 1.0 / cs.initial_count, 4) AS retention_rate
FROM tracked_periods tp
CROSS JOIN cohort_size cs
LEFT JOIN retention_cte r ON tp.year_num = r.year_num
ORDER BY tp.year_num ASC;
