WITH yearly_stats AS (
  SELECT
    strftime('%Y', date) AS year,
    category,
    ROUND(AVG(whsle_price), 2) AS avg_whsle_price,
    ROUND(MAX(whsle_price), 2) AS max_whsle_price,
    ROUND(MIN(whsle_price), 2) AS min_whsle_price,
    ROUND(MAX(whsle_price) - MIN(whsle_price), 2) AS whsle_price_diff,
    ROUND(SUM(whsle_price), 2) AS total_whsle_price,
    ROUND(SUM(sell_price), 2) AS total_sell_price,
    ROUND(AVG(loss_rate), 2) AS avg_loss_rate,
    ROUND(SUM(loss), 2) AS total_loss,
    ROUND(SUM(sell_price) - SUM(whsle_price) - SUM(loss), 2) AS profit
  FROM veg_whsle_df
  WHERE year BETWEEN '2020' AND '2023'
  GROUP BY year, category
  ORDER BY year, category
)
SELECT
  year,
  category,
  avg_whsle_price,
  max_whsle_price,
  min_whsle_price,
  whsle_price_diff,
  total_whsle_price,
  total_sell_price,
  avg_loss_rate,
  total_loss,
  profit
FROM yearly_stats;
