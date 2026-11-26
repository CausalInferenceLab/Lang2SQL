WITH daily_insertions AS (
    SELECT country_code_2,
           DATE(insert_date) AS insertion_day,
           COUNT(*) AS entry_count,
           COUNT(CASE WHEN capital = 1 THEN 1 END) AS capital_entry_count
    FROM cities
    WHERE DATE(insert_date) >= '2022-01-01'
      AND DATE(insert_date) < '2022-02-01'
    GROUP BY country_code_2, DATE(insert_date)
), numbered_days AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY country_code_2) AS january_insertion_days,
           JULIANDAY(insertion_day) - ROW_NUMBER() OVER (
               PARTITION BY country_code_2 ORDER BY insertion_day
           ) AS streak_key
    FROM daily_insertions
), ranked_streaks AS (
    SELECT country_code_2,
           MIN(insertion_day) AS period_start,
           MAX(insertion_day) AS period_end,
           1.0 * SUM(capital_entry_count) / SUM(entry_count) AS capital_proportion,
           DENSE_RANK() OVER (
               PARTITION BY country_code_2 ORDER BY COUNT(*) DESC
           ) AS streak_rank
    FROM numbered_days
    WHERE january_insertion_days = 9
    GROUP BY country_code_2, streak_key
)
SELECT c.country_name,
       s.period_start,
       s.period_end,
       s.capital_proportion
FROM ranked_streaks AS s
JOIN cities_countries AS c ON c.country_code_2 = s.country_code_2
WHERE s.streak_rank = 1
ORDER BY c.country_name, s.period_start;
