WITH RECURSIVE periods(period) AS (
    SELECT 1
    UNION ALL
    SELECT period + 1 FROM periods WHERE period < 20
),
first_terms AS (
    SELECT id_bioguide, MIN(term_start) AS first_term_start
    FROM legislators_terms
    GROUP BY id_bioguide
),
cohort AS (
    SELECT id_bioguide, CAST(strftime('%Y', first_term_start) AS INTEGER) AS start_year
    FROM first_terms
    WHERE first_term_start >= '1917-01-01'
      AND first_term_start < '2000-01-01'
)
SELECT p.period,
       1.0 * COUNT(DISTINCT t.id_bioguide)
           / NULLIF((SELECT COUNT(*) FROM cohort), 0) AS retention_rate
FROM periods AS p
LEFT JOIN cohort AS c ON 1 = 1
LEFT JOIN legislators_terms AS t
    ON t.id_bioguide = c.id_bioguide
   AND printf('%04d-12-31', c.start_year + p.period - 1)
       BETWEEN t.term_start AND t.term_end
GROUP BY p.period
ORDER BY p.period;
