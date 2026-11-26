SELECT ROUND(
    100.0 * COUNT(DISTINCT CASE
        WHEN LOWER(TRIM(pa.defensive_work_rate)) = 'high' THEN p.player_api_id
    END) / NULLIF(COUNT(DISTINCT p.player_api_id), 0),
    2
) AS high_defensive_work_rate_percentage
FROM Player AS p
JOIN Player_Attributes AS pa ON pa.player_api_id = p.player_api_id
WHERE p.birthday >= '1991-01-01'
  AND LOWER(TRIM(pa.defensive_work_rate)) IN ('low', 'medium', 'high');
