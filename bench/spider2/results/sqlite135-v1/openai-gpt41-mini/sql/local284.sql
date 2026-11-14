WITH stats AS (
  SELECT 
    AVG(loss_rate) AS avg_loss_rate,
    STDDEV(loss_rate) AS stddev_loss_rate
  FROM veg_loss_rate_df
),
categorized AS (
  SELECT
    CASE
      WHEN loss_rate < avg_loss_rate - stddev_loss_rate THEN 'below'
      WHEN loss_rate > avg_loss_rate + stddev_loss_rate THEN 'above'
      ELSE 'within'
    END AS category
  FROM veg_loss_rate_df, stats
)
SELECT
  avg_loss_rate,
  COUNT(CASE WHEN category = 'below' THEN 1 END) AS count_below_stddev,
  COUNT(CASE WHEN category = 'within' THEN 1 END) AS count_within_stddev,
  COUNT(CASE WHEN category = 'above' THEN 1 END) AS count_above_stddev
FROM stats, categorized;
