WITH RankedCompositions AS (
  SELECT
    month_year AS time,
    interest_id,
    composition,
    RANK() OVER (PARTITION BY month_year ORDER BY composition DESC) AS rank_desc,
    RANK() OVER (PARTITION BY month_year ORDER BY composition ASC) AS rank_asc
  FROM interest_metrics
),
InterestNames AS (
  SELECT id AS interest_id, interest_name
  FROM interest_map
)
SELECT rc.time, im.interest_name, rc.composition
FROM RankedCompositions rc
JOIN InterestNames im ON rc.interest_id = im.interest_id
WHERE rc.rank_desc <= 10 OR rc.rank_asc <= 10
ORDER BY rc.composition DESC, rc.time, im.interest_name;
