WITH la_male_legislators AS (
  SELECT l.id_bioguide, l.gender, lt.state, MIN(lt.term_start) AS first_term_start
  FROM legislators l
  JOIN legislators_terms lt ON l.id_bioguide = lt.id_bioguide
  WHERE l.gender = 'M' AND lt.state = 'LA'
  GROUP BY l.id_bioguide, l.gender, lt.state
),
active_on_year_end AS (
  SELECT lml.id_bioguide,
         CAST(strftime('%Y', year_ends.end_of_year) AS INTEGER) - CAST(strftime('%Y', lml.first_term_start) AS INTEGER) AS years_elapsed
  FROM la_male_legislators lml
  JOIN legislators_terms lt ON lml.id_bioguide = lt.id_bioguide
  JOIN (
    SELECT DISTINCT DATE(year || '-12-31') AS end_of_year
    FROM (
      SELECT CAST(strftime('%Y', term_start) AS INTEGER) AS year FROM legislators_terms
      UNION
      SELECT CAST(strftime('%Y', term_end) AS INTEGER) AS year FROM legislators_terms
      WHERE term_end IS NOT NULL
    ) WHERE year IS NOT NULL
  ) AS year_ends ON lt.term_start <= year_ends.end_of_year AND (lt.term_end IS NULL OR lt.term_end >= year_ends.end_of_year)
)
SELECT years_elapsed, COUNT(DISTINCT id_bioguide) AS distinct_legislators
FROM active_on_year_end
WHERE years_elapsed > 30 AND years_elapsed < 50
GROUP BY years_elapsed
ORDER BY years_elapsed;
