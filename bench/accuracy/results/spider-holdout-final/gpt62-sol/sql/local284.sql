WITH item_rates AS (
    SELECT l."loss_rate_%" AS loss_rate
    FROM veg_loss_rate_df AS l
    JOIN (SELECT DISTINCT item_code FROM veg_whsle_df) AS w
      ON w.item_code = l.item_code
), mean_rate AS (
    SELECT AVG(loss_rate) AS average_loss_rate
    FROM item_rates
), stats AS (
    SELECT m.average_loss_rate,
           SQRT(AVG((r.loss_rate - m.average_loss_rate) *
                    (r.loss_rate - m.average_loss_rate))) AS stddev
    FROM mean_rate AS m
    LEFT JOIN item_rates AS r ON 1 = 1
    GROUP BY m.average_loss_rate
)
SELECT s.average_loss_rate,
       COUNT(CASE WHEN r.loss_rate < s.average_loss_rate - s.stddev THEN 1 END) AS items_below_one_stddev,
       COUNT(CASE WHEN r.loss_rate > s.average_loss_rate + s.stddev THEN 1 END) AS items_above_one_stddev,
       COUNT(CASE WHEN r.loss_rate BETWEEN s.average_loss_rate - s.stddev
                                      AND s.average_loss_rate + s.stddev THEN 1 END) AS items_within_one_stddev
FROM stats AS s
LEFT JOIN item_rates AS r ON 1 = 1
GROUP BY s.average_loss_rate;
