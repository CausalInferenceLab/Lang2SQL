WITH RECURSIVE periods(period) AS (
    SELECT 1
    UNION ALL
    SELECT period + 1
    FROM periods
    WHERE period < 20
),
cohort AS (
    SELECT id_bioguide, MIN(term_start) AS first_term_start
    FROM main.legislators_terms
    GROUP BY id_bioguide
    HAVING MIN(term_start) BETWEEN '1917-01-01' AND '1999-12-31'
),
snapshots AS (
    SELECT
        periods.period,
        cohort.id_bioguide,
        printf(
            '%04d-12-31',
            CAST(strftime('%Y', cohort.first_term_start) AS INTEGER) + periods.period
        ) AS snapshot_date
    FROM periods
    CROSS JOIN cohort
),
retained AS (
    SELECT snapshots.period, COUNT(DISTINCT snapshots.id_bioguide) AS retained_count
    FROM snapshots
    JOIN main.legislators_terms AS terms
      ON terms.id_bioguide = snapshots.id_bioguide
     AND snapshots.snapshot_date BETWEEN terms.term_start AND terms.term_end
    GROUP BY snapshots.period
)
SELECT
    periods.period,
    1.0 * COALESCE(retained.retained_count, 0)
        / NULLIF((SELECT COUNT(*) FROM cohort), 0) AS retention_rate
FROM periods
LEFT JOIN retained ON retained.period = periods.period
ORDER BY periods.period
LIMIT 20
