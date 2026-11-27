WITH ranked_grades AS (
    SELECT
        player_api_id,
        LOWER(TRIM(defensive_work_rate)) AS defensive_grade,
        ROW_NUMBER() OVER (
            PARTITION BY player_api_id
            ORDER BY date DESC, id DESC
        ) AS rn
    FROM Player_Attributes
    WHERE LOWER(TRIM(defensive_work_rate)) IN ('low', 'medium', 'high')
)
SELECT ROUND(AVG(p.height), 2) AS average_height_cm
FROM Player AS p
JOIN ranked_grades AS g ON g.player_api_id = p.player_api_id
WHERE p.birthday >= '1990-01-01'
  AND p.birthday < '1996-01-01'
  AND g.rn = 1
  AND g.defensive_grade = 'medium';
