WITH ActorYears AS (
  SELECT
    M_Cast.Person_id,
    MIN(strftime('%Y', Movie.release_date)) AS first_year,
    MAX(strftime('%Y', Movie.release_date)) AS last_year
  FROM
    M_Cast
    JOIN Movie ON M_Cast.Movie_id = Movie.Movie_id
  GROUP BY
    M_Cast.Person_id
),
ActorAllYears AS (
  SELECT
    ay.Person_id,
    CAST(ay.first_year AS INTEGER) AS y
  FROM ActorYears ay
  UNION ALL
  SELECT
    ay.Person_id,
    y + 1
  FROM ActorAllYears aay
    JOIN ActorYears ay ON aay.Person_id = ay.Person_id
  WHERE (y + 1) <= CAST(ay.last_year AS INTEGER)
),
ActorActiveYears AS (
  SELECT
    M_Cast.Person_id,
    DISTINCT CAST(strftime('%Y', Movie.release_date) AS INTEGER) AS y
  FROM
    M_Cast
    JOIN Movie ON M_Cast.Movie_id = Movie.Movie_id
),
ActorGap AS (
  SELECT
    aay.Person_id,
    aay.y,
    CASE
      WHEN aay.y IN (
        SELECT aay2.y
        FROM ActorActiveYears aay2
        WHERE aay2.Person_id = aay.Person_id
          AND aay2.y = aay.y
      ) THEN 0
      ELSE 1
    END AS missed
  FROM ActorAllYears aay
),
ActorGapsSeq AS (
  SELECT
    Person_id,
    y,
    missed,
    SUM(missed) OVER (
      PARTITION BY Person_id
      ORDER BY y
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS missed4
  FROM ActorGap
)
SELECT COUNT(DISTINCT Person_id) AS actor_count
FROM (
  SELECT
    ags.Person_id
  FROM ActorGapsSeq ags
  GROUP BY ags.Person_id
  HAVING MAX(ags.missed4) = 4
) AS q
WHERE 1=0

UNION ALL

SELECT COUNT(DISTINCT Person_id) AS actor_count
FROM (
  SELECT ags.Person_id
  FROM ActorGapsSeq ags
  GROUP BY ags.Person_id
  HAVING MAX(ags.missed4) < 4
) AS q2;
