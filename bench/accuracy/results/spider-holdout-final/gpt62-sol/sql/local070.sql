WITH july_entries AS (
    SELECT date(insert_date) AS entry_date, city_id, city_name
    FROM cities
    WHERE country_code_2 = 'cn'
      AND date(insert_date) >= '2021-07-01'
      AND date(insert_date) <  '2021-08-01'
), ranked_entries AS (
    SELECT entry_date, city_name,
           ROW_NUMBER() OVER (PARTITION BY entry_date ORDER BY city_id, city_name) AS daily_rank
    FROM july_entries
), streak_days AS (
    SELECT entry_date, city_name,
           julianday(entry_date) - ROW_NUMBER() OVER (ORDER BY entry_date) AS streak_id
    FROM ranked_entries
    WHERE daily_rank = 1
), streak_lengths AS (
    SELECT entry_date, city_name,
           COUNT(*) OVER (PARTITION BY streak_id) AS streak_length
    FROM streak_days
), extremes AS (
    SELECT MIN(streak_length) AS shortest, MAX(streak_length) AS longest
    FROM streak_lengths
)
SELECT entry_date AS insert_date,
       upper(substr(city_name, 1, 1)) || lower(substr(city_name, 2)) AS city_name
FROM streak_lengths
CROSS JOIN extremes
WHERE streak_length IN (shortest, longest)
ORDER BY entry_date;
