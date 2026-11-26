SELECT ROUND(AVG(p.weight), 1) AS average_weight
FROM Player AS p
WHERE p.player_api_id IN (
    SELECT pa.player_api_id
    FROM Player_Attributes AS pa
    WHERE pa.preferred_foot = 'left'
);
