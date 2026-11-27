WITH first_terms AS (
    SELECT id_bioguide, state, term_start,
           ROW_NUMBER() OVER (
               PARTITION BY id_bioguide ORDER BY term_start, term_number, term_id
           ) AS term_order
    FROM legislators_terms
    WHERE term_start IS NOT NULL
),
intervals(years_after_start) AS (
    VALUES (0), (2), (4), (6), (8), (10)
),
checkpoints AS (
    SELECT f.id_bioguide, f.state, l.gender, i.years_after_start,
           date(f.term_start, 'start of year',
                printf('+%d years', i.years_after_start), '+1 year', '-1 day') AS dec_31
    FROM first_terms AS f
    JOIN legislators AS l ON l.id_bioguide = f.id_bioguide
    CROSS JOIN intervals AS i
    WHERE f.term_order = 1
      AND f.state IS NOT NULL
      AND l.gender IN ('M', 'F')
),
nonzero_retention AS (
    SELECT DISTINCT c.state, c.gender, c.years_after_start
    FROM checkpoints AS c
    JOIN legislators_terms AS t
      ON t.id_bioguide = c.id_bioguide
     AND c.dec_31 BETWEEN t.term_start AND t.term_end
)
SELECT state
FROM nonzero_retention
GROUP BY state
HAVING COUNT(*) = 12
ORDER BY state;
