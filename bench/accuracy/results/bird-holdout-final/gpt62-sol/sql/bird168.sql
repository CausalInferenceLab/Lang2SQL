SELECT
    AVG(CASE WHEN p.player_name = 'Abdou Diallo' THEN pa.ball_control END)
    - AVG(CASE WHEN p.player_name = 'Aaron Appindangoye' THEN pa.ball_control END)
    AS mean_ball_control_difference
FROM Player AS p
JOIN Player_Attributes AS pa ON pa.player_api_id = p.player_api_id
WHERE p.player_name IN ('Abdou Diallo', 'Aaron Appindangoye');
