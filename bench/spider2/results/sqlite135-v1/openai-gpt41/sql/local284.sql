WITH stats AS (
  SELECT 
    AVG("loss_rate_%") AS avg_loss_rate, 
    SQRT(AVG(("loss_rate_%" - (SELECT AVG("loss_rate_%") FROM veg_loss_rate_df)) * ("loss_rate_%" - (SELECT AVG("loss_rate_%") FROM veg_loss_rate_df)))) AS std_loss_rate
  FROM veg_loss_rate_df
),
classified AS (
  SELECT 
    item_code,
    item_name,
    "loss_rate_%",
    (SELECT avg_loss_rate FROM stats) AS avg_loss_rate,
    (SELECT std_loss_rate FROM stats) AS std_loss_rate,
    CASE 
      WHEN "loss_rate_%" < (SELECT avg_loss_rate - std_loss_rate FROM stats) THEN 'below one std dev'
      WHEN "loss_rate_%" > (SELECT avg_loss_rate + std_loss_rate FROM stats) THEN 'above one std dev'
      ELSE 'within one std dev'
    END AS deviation_category
  FROM veg_loss_rate_df
)
SELECT 
  (SELECT avg_loss_rate FROM stats) AS avg_loss_rate,
  (SELECT std_loss_rate FROM stats) AS std_loss_rate,
  SUM(deviation_category = 'below one std dev') AS count_below_one_stddev,
  SUM(deviation_category = 'within one std dev') AS count_within_one_stddev,
  SUM(deviation_category = 'above one std dev') AS count_above_one_stddev
FROM classified;
