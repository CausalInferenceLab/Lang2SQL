WITH ranked_terms AS (
    SELECT t.id_bioguide,
           t.state,
           ROW_NUMBER() OVER (
               PARTITION BY t.id_bioguide
               ORDER BY t.term_start, t.term_number, t.state
           ) AS term_rank
    FROM legislators_terms AS t
    WHERE EXISTS (
        SELECT 1
        FROM legislators AS l
        WHERE l.id_bioguide = t.id_bioguide
          AND l.gender = 'F'
    )
)
SELECT r.state AS state_abbreviation,
       COUNT(*) AS legislator_count
FROM ranked_terms AS r
WHERE r.term_rank = 1
  AND EXISTS (
      SELECT 1
      FROM legislators_terms AS t
      WHERE t.id_bioguide = r.id_bioguide
        AND date(t.term_start, 'start of year', '+1 year', '-1 day')
            BETWEEN date(t.term_start) AND date(t.term_end)
  )
GROUP BY r.state
ORDER BY legislator_count DESC, state_abbreviation
LIMIT 1;
