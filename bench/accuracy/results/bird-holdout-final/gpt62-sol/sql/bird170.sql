WITH crossing_records AS (
    SELECT pa.date AS recorded_at, pa.crossing
    FROM Player AS p
    JOIN Player_Attributes AS pa ON pa.player_api_id = p.player_api_id
    WHERE p.player_name = 'Kevin Constant'
)
SELECT DATE(MIN(recorded_at)) AS first_highest_crossing_date
FROM crossing_records
WHERE crossing = (SELECT MAX(crossing) FROM crossing_records);
