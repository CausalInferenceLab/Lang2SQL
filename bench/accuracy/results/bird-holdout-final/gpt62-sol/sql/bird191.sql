SELECT COUNT(DISTINCT player_api_id) AS player_count
FROM Player_Attributes
WHERE jumping >= 80
  AND DATE(date) BETWEEN '2007-02-22' AND '2007-08-30';
