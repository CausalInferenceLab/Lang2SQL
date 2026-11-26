WITH item_rates AS (
    SELECT "loss_rate_%" AS rate
    FROM main.veg_loss_rate_df
    WHERE item_code IN (SELECT item_code FROM main.veg_whsle_df)
),
mean_rate AS (
    SELECT AVG(rate) AS mean_pct
    FROM item_rates
),
spread AS (
    SELECT SQRT(AVG((rate - mean_pct) * (rate - mean_pct))) AS sd
    FROM item_rates CROSS JOIN mean_rate
)
SELECT
    AVG(rate) AS average_loss_rate_pct,
    COUNT(*) FILTER (WHERE rate < mean_pct - sd) AS items_below_one_sd,
    COUNT(*) FILTER (WHERE rate > mean_pct + sd) AS items_above_one_sd,
    COUNT(*) FILTER (WHERE rate BETWEEN mean_pct - sd AND mean_pct + sd) AS items_within_one_sd
FROM item_rates CROSS JOIN mean_rate CROSS JOIN spread
LIMIT 1
