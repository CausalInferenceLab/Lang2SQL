WITH FemaleLegislators AS (
  SELECT id_bioguide
  FROM legislators
  WHERE gender = 'F'
), FemaleTermStates AS (
  SELECT 
    ft.id_bioguide,
    ft.state,
    MIN(ft.term_start) AS first_term_start
  FROM legislators_terms ft
  JOIN FemaleLegislators fl ON ft.id_bioguide = fl.id_bioguide
  GROUP BY ft.id_bioguide, ft.state
), FirstStatePerFemale AS (
  SELECT id_bioguide, state AS first_state
  FROM (
    SELECT id_bioguide, state, first_term_start,
           ROW_NUMBER() OVER (PARTITION BY id_bioguide ORDER BY first_term_start) AS rn
    FROM FemaleTermStates
  ) WHERE rn = 1
), TermsIncludingDec31 AS (
  SELECT l.id_bioguide, f.first_state
  FROM legislators_terms l
  JOIN FirstStatePerFemale f ON l.id_bioguide = f.id_bioguide
  WHERE (
    (strftime('%m-%d', l.term_start) <= '12-31' AND (l.term_end IS NULL OR strftime('%m-%d', l.term_end) >= '12-31'))
  )
)
SELECT first_state AS state_abbreviation, COUNT(DISTINCT id_bioguide) AS female_legislators_count
FROM TermsIncludingDec31
GROUP BY first_state
ORDER BY female_legislators_count DESC
LIMIT 1;
