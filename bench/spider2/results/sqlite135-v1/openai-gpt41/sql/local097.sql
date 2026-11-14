WITH clean_years AS (
  SELECT CAST(substr(year, -4, 4) AS INTEGER) AS clean_year
  FROM Movie
  WHERE substr(year, -4, 4) GLOB '[0-9][0-9][0-9][0-9]'
),
window_counts AS (
  SELECT
    cy.clean_year AS window_start,
    (
      SELECT COUNT(*)
      FROM clean_years c2
      WHERE c2.clean_year BETWEEN cy.clean_year AND cy.clean_year+9
    ) AS film_count
  FROM (SELECT DISTINCT clean_year FROM clean_years) cy
)
SELECT window_start, film_count
FROM window_counts
ORDER BY film_count DESC, window_start ASC
LIMIT 1;
