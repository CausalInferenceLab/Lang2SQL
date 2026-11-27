WITH daily AS (
    SELECT country_code_2, DATE(insert_date) AS day,
           COUNT(*) AS entries,
           SUM(CASE WHEN capital = 1 THEN 1 ELSE 0 END) AS capital_entries
    FROM cities
    WHERE insert_date >= '2022-01-01' AND insert_date < '2022-02-01'
    GROUP BY country_code_2, DATE(insert_date)
), numbered AS (
    SELECT daily.*,
           COUNT(*) OVER (PARTITION BY country_code_2) AS insertion_days,
           JULIANDAY(day) - ROW_NUMBER() OVER (
               PARTITION BY country_code_2 ORDER BY day
           ) AS streak_key
    FROM daily
), streaks AS (
    SELECT country_code_2, MIN(day) AS start_date, MAX(day) AS end_date,
           COUNT(*) AS streak_days,
           1.0 * SUM(capital_entries) / SUM(entries) AS capital_proportion
    FROM numbered
    WHERE insertion_days = 9
    GROUP BY country_code_2, streak_key
), ranked AS (
    SELECT streaks.*, RANK() OVER (
        PARTITION BY country_code_2 ORDER BY streak_days DESC
    ) AS streak_rank
    FROM streaks
)
SELECT c.country_name, r.start_date, r.end_date, r.capital_proportion
FROM ranked AS r
JOIN cities_countries AS c ON c.country_code_2 = r.country_code_2
WHERE r.streak_rank = 1
ORDER BY c.country_name, r.start_date;
