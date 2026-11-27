WITH shahrukh AS (
    SELECT TRIM(PID) AS pid
    FROM Person
    WHERE TRIM(Name) = 'Shah Rukh Khan'
), cast_credits AS (
    SELECT MID, TRIM(PID) AS pid
    FROM M_Cast
    WHERE PID IS NOT NULL
), within_one_hop AS (
    SELECT DISTINCT coactor.pid
    FROM shahrukh AS s
    JOIN cast_credits AS starring ON starring.pid = s.pid
    JOIN cast_credits AS coactor ON coactor.MID = starring.MID
), exactly_two_hops AS (
    SELECT candidate.pid
    FROM within_one_hop AS w
    JOIN cast_credits AS bridge ON bridge.pid = w.pid
    JOIN cast_credits AS candidate ON candidate.MID = bridge.MID
    EXCEPT
    SELECT pid FROM within_one_hop
)
SELECT COUNT(*) AS actor_count
FROM exactly_two_hops;
