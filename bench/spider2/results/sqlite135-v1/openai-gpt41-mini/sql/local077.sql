WITH InterestData AS (
  SELECT
    month_year AS month,
    interest_id AS interest,
    composition / index_value AS avg_composition
  FROM interest_metrics
  WHERE month_year >= '2018-09' AND month_year <= '2019-08'
),
MaxInterestPerMonth AS (
  SELECT
    month,
    interest AS max_interest,
    MAX(avg_composition) AS max_index_composition
  FROM InterestData
  GROUP BY month
),
MaxIndexWithRolling AS (
  SELECT
    m1.month,
    m1.max_interest,
    m1.max_index_composition,
    AVG(m2.max_index_composition) OVER(ORDER BY m1.month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_avg_max_index_composition
  FROM MaxInterestPerMonth m1
  LEFT JOIN MaxInterestPerMonth m2 ON m2.month <= m1.month
),
MaxInterestPriorMonths AS (
  SELECT
    m.month,
    m.max_interest,
    m.max_index_composition,
    LAG(m.max_interest, 1) OVER(ORDER BY m.month) AS interest_1_month_ago,
    LAG(m.max_index_composition, 1) OVER(ORDER BY m.month) AS max_index_composition_1_month_ago,
    LAG(m.max_interest, 2) OVER(ORDER BY m.month) AS interest_2_month_ago,
    LAG(m.max_index_composition, 2) OVER(ORDER BY m.month) AS max_index_composition_2_month_ago
  FROM MaxInterestPerMonth m
)
SELECT
  m.month AS date,
  m.max_interest AS interest_name,
  m.max_index_composition,
  r.rolling_avg_max_index_composition AS three_month_rolling_avg,
  p.interest_1_month_ago AS interest_1m_ago,
  p.max_index_composition_1_month_ago AS max_index_composition_1m_ago,
  p.interest_2_month_ago AS interest_2m_ago,
  p.max_index_composition_2_month_ago AS max_index_composition_2m_ago
FROM MaxInterestPerMonth m
JOIN MaxIndexWithRolling r ON m.month = r.month
JOIN MaxInterestPriorMonths p ON m.month = p.month
ORDER BY m.month;
