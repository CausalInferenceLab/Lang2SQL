WITH january_daily AS (
    SELECT country_code_2,
           DATE(insert_date) AS insertion_day,
           COUNT(*) AS entry_count,
           SUM(CASE WHEN capital = 1 THEN 1 ELSE 0 END) AS capital_entry_count
    FROM cities
    WHERE DATE(insert_date) >= '2022-01-01'
      AND DATE(insert_date) < '2022-02-01'
      AND country_code_2 IS NOT NULL
    GROUP BY country_code_2, DATE(insert_date)
), numbered_days AS (
    SELECT *,
           COUNT(*) OVER (PARTITION BY country_code_2) AS january_days,
           JULIANDAY(insertion_day) - ROW_NUMBER() OVER (
               PARTITION BY country_code_2 ORDER BY insertion_day
           ) AS streak_id
    FROM january_daily
), streaks AS (
    SELECT country_code_2,
           MIN(insertion_day) AS period_start,
           MAX(insertion_day) AS period_end,
           COUNT(*) AS consecutive_days,
           1.0 * SUM(capital_entry_count) / SUM(entry_count) AS capital_proportion
    FROM numbered_days
    WHERE january_days = 9
    GROUP BY country_code_2, streak_id
), ranked_streaks AS (
    SELECT *,
           RANK() OVER (
               PARTITION BY country_code_2 ORDER BY consecutive_days DESC
           ) AS streak_rank
    FROM streaks
)
SELECT c.country_name,
       s.period_start,
       s.period_end,
       s.consecutive_days,
       s.capital_proportion
FROM ranked_streaks AS s
JOIN cities_countries AS c ON c.country_code_2 = s.country_code_2
WHERE s.streak_rank = 1
ORDER BY c.country_name, s.period_start;
