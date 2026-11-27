WITH january_entries AS (
    SELECT country_code_2, DATE(insert_date) AS insertion_day, capital
    FROM cities
    WHERE DATE(insert_date) >= '2022-01-01'
      AND DATE(insert_date) < '2022-02-01'
), insertion_days AS (
    SELECT DISTINCT country_code_2, insertion_day
    FROM january_entries
), matching_countries AS (
    SELECT country_code_2
    FROM insertion_days
    GROUP BY country_code_2
    HAVING COUNT(*) = 9
), numbered_days AS (
    SELECT d.country_code_2, d.insertion_day,
           JULIANDAY(d.insertion_day) - ROW_NUMBER() OVER (
               PARTITION BY d.country_code_2 ORDER BY d.insertion_day
           ) AS streak_key
    FROM insertion_days AS d
    JOIN matching_countries AS m ON m.country_code_2 = d.country_code_2
), streaks AS (
    SELECT country_code_2, MIN(insertion_day) AS period_start,
           MAX(insertion_day) AS period_end, COUNT(*) AS streak_days
    FROM numbered_days
    GROUP BY country_code_2, streak_key
), ranked_streaks AS (
    SELECT country_code_2, period_start, period_end,
           DENSE_RANK() OVER (
               PARTITION BY country_code_2 ORDER BY streak_days DESC
           ) AS streak_rank
    FROM streaks
)
SELECT c.country_name, s.period_start, s.period_end,
       AVG(CASE WHEN e.capital = 1 THEN 1.0 ELSE 0.0 END) AS capital_proportion
FROM ranked_streaks AS s
JOIN january_entries AS e
  ON e.country_code_2 = s.country_code_2
 AND e.insertion_day BETWEEN s.period_start AND s.period_end
JOIN cities_countries AS c ON c.country_code_2 = s.country_code_2
WHERE s.streak_rank = 1
GROUP BY s.country_code_2, c.country_name, s.period_start, s.period_end;
