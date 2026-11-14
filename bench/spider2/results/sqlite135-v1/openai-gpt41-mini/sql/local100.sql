WITH ShahrukhPID AS (
  SELECT mc.PID
  FROM Person p
  JOIN M_Cast mc ON p.ID = mc.ID
  WHERE p.name = 'Shahrukh Khan'
  LIMIT 1
),
ShahrukhCoActors AS (
  SELECT DISTINCT mc1.PID
  FROM M_Cast mc1
  JOIN M_Cast mc2 ON mc1.MID = mc2.MID
  WHERE mc2.PID = (SELECT PID FROM ShahrukhPID)
  AND mc1.PID != mc2.PID
),
ActorsWithShahrukhNumber2 AS (
  SELECT DISTINCT mc1.PID
  FROM M_Cast mc1
  JOIN M_Cast mc2 ON mc1.MID = mc2.MID
  WHERE mc2.PID IN (SELECT PID FROM ShahrukhCoActors)
  AND mc1.PID NOT IN (SELECT PID FROM ShahrukhCoActors)
  AND mc1.PID != (SELECT PID FROM ShahrukhPID)
)
SELECT COUNT(*) AS actors_with_shahrukh_number_2
FROM ActorsWithShahrukhNumber2;
