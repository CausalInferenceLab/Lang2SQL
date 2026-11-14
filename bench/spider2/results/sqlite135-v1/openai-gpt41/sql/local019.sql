SELECT w1.name AS wrestler_1, w2.name AS wrestler_2
FROM Matches m
JOIN Match_Types mt ON m.match_type_id = mt.id
JOIN Wrestlers w1 ON m.wrestler1_id = w1.id
JOIN Wrestlers w2 ON m.wrestler2_id = w2.id
WHERE mt.name LIKE '%NXT%'
  AND m.result NOT LIKE '%title change%'
ORDER BY m.duration ASC
LIMIT 1;
