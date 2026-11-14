WITH CauseCountsByYear AS (
  SELECT
    strftime('%Y', collisions.collision_date) AS year,
    collisions.primary_collision_factor AS cause,
    COUNT(*) AS count
  FROM collisions
  GROUP BY year, collisions.primary_collision_factor
),
TopCausesByYear AS (
  SELECT
    year,
    cause,
    count,
    RANK() OVER (PARTITION BY year ORDER BY count DESC) AS cause_rank
  FROM CauseCountsByYear
  WHERE cause IS NOT NULL
),
YearTop2Causes AS (
  SELECT
    year,
    cause
  FROM TopCausesByYear
  WHERE cause_rank <= 2
),
YearTop2CauseString AS (
  SELECT
    year,
    GROUP_CONCAT(cause, ',') AS causes
  FROM YearTop2Causes
  GROUP BY year
),
MostCommonCauseString AS (
  SELECT
    causes,
    COUNT(*) AS cnt
  FROM YearTop2CauseString
  GROUP BY causes
  ORDER BY cnt DESC
  LIMIT 1
),
YearsWithDiffCauses AS (
  SELECT
    year
  FROM YearTop2CauseString
  WHERE causes != (SELECT causes FROM MostCommonCauseString)
)
SELECT year
FROM YearsWithDiffCauses;
