WITH first_terms AS (
    SELECT id_bioguide, MIN(term_start) AS first_term_start
    FROM legislators_terms
    GROUP BY id_bioguide
), year_end_service AS (
    SELECT lt.id_bioguide,
           CAST(strftime('%Y', d.date) AS INTEGER)
           - CAST(strftime('%Y', ft.first_term_start) AS INTEGER) AS years_since_first_term
    FROM legislators_terms AS lt
    JOIN first_terms AS ft ON ft.id_bioguide = lt.id_bioguide
    JOIN legislators AS l ON l.id_bioguide = lt.id_bioguide
    JOIN legislation_date_dim AS d ON d.date BETWEEN lt.term_start AND lt.term_end
    WHERE l.gender = 'M'
      AND lt.state = 'LA'
      AND d.month_name = 'December'
      AND d.day_of_month = 31
)
SELECT years_since_first_term,
       COUNT(DISTINCT id_bioguide) AS legislator_count
FROM year_end_service
WHERE years_since_first_term > 30 AND years_since_first_term < 50
GROUP BY years_since_first_term
ORDER BY years_since_first_term;
