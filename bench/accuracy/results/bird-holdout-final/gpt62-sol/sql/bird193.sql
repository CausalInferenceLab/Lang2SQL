WITH ranked_ratings AS (
    SELECT player_api_id, overall_rating, defensive_work_rate,
           ROW_NUMBER() OVER (
               PARTITION BY player_api_id
               ORDER BY date DESC, id DESC
           ) AS rn
    FROM Player_Attributes
    WHERE date < '2013-07-13'
)
SELECT ROUND(
           100.0 * (MAX(CASE WHEN p.player_name = 'Landon Donovan' THEN r.overall_rating END)
                    - AVG(r.overall_rating)) / AVG(r.overall_rating),
           2
       ) AS percentage_higher
FROM ranked_ratings AS r
JOIN Player AS p ON p.player_api_id = r.player_api_id
WHERE r.rn = 1
  AND LOWER(TRIM(r.defensive_work_rate)) IN ('low', 'medium', 'high');
