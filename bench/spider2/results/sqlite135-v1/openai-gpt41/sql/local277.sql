WITH monthly_sales_filtered AS (
  SELECT 
    strftime('%Y-%m', order_date) AS month,
    product_id,
    SUM(amount) AS sales
  FROM monthly_sales
  WHERE product_id IN (4160, 7790)
    AND order_date >= '2016-01-01'
    AND order_date < '2019-01-01'
  GROUP BY product_id, month
),
-- Assign a continuous time step 1...36 for each (product, month) from Jan 2016
sales_with_timestep AS (
  SELECT 
    a.*,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY month) AS time_step
  FROM monthly_sales_filtered a
),
-- Compute Centered Moving Average (CMA) using 12-month overlapping windows for time steps 7~30
cma_calc AS (
  SELECT 
    s1.product_id,
    s1.month,
    s1.time_step,
    s1.sales,
    (
      -- First window: avg(sales) from t-5 to t+6
      (SELECT AVG(s2.sales)
        FROM sales_with_timestep s2
        WHERE s2.product_id = s1.product_id 
          AND s2.time_step BETWEEN s1.time_step - 5 AND s1.time_step + 6)
      +
      -- Second window: avg(sales) from t-6 to t+5
      (SELECT AVG(s3.sales)
        FROM sales_with_timestep s3
        WHERE s3.product_id = s1.product_id 
          AND s3.time_step BETWEEN s1.time_step - 6 AND s1.time_step + 5)
    ) / 2.0 AS cma
  FROM sales_with_timestep s1
  WHERE s1.time_step BETWEEN 7 AND 30
),
seasonality_factors AS (
  SELECT
    product_id,
    ((sales / NULLIF(cma, 0))) AS sales_to_cma,
    strftime('%m', month) AS month_num
  FROM cma_calc
),
seasonality_adj AS (
  SELECT 
    product_id,
    month_num, 
    AVG(sales_to_cma) AS seasonality
  FROM seasonality_factors
  GROUP BY product_id, month_num
),
-- Detrend each point by removing seasonality from the original 36m
detrended AS (
  SELECT
    s.product_id,
    s.month,
    s.time_step,
    s.sales / NULLIF(sa.seasonality, 0) AS detrended_sales
  FROM sales_with_timestep s
  LEFT JOIN seasonality_adj sa
    ON s.product_id = sa.product_id AND strftime('%m', s.month) = sa.month_num
  WHERE s.time_step BETWEEN 1 AND 36
),
-- Fit a weighted linear regression for trend estimation (weights = t for recent months), using all 36 months
regression AS (
  SELECT 
    product_id,
    -- Weighted regression coefficients (beta for slope, alpha for intercept)
    -- beta numerator: SUM(weight * (x - x_avg) * (y - y_avg)), denominator: SUM(weight * (x - x_avg)^2)
    -- alpha: y_avg - beta * x_avg
    SUM(weight * (time_step - x_avg) * (detrended_sales - y_avg)) / NULLIF(SUM(weight * (time_step - x_avg)*(time_step - x_avg)),0) AS beta,
    (y_avg - (
      SUM(weight * (time_step - x_avg) * (detrended_sales - y_avg)) / NULLIF(SUM(weight * (time_step - x_avg)*(time_step - x_avg)),0)
    ) * x_avg) AS alpha,
    x_avg, y_avg
  FROM (
    SELECT 
      d.*,
      d.time_step AS weight,
      -- Precompute averages
      (SELECT AVG(time_step) FROM detrended d2 WHERE d2.product_id = d.product_id) AS x_avg,
      (SELECT AVG(detrended_sales) FROM detrended d3 WHERE d3.product_id = d.product_id) AS y_avg
    FROM detrended d
  )
  GROUP BY product_id
),
-- Forecast raw sales for each month in 2018 (time_step 25~36), reapply seasonality
forecast_2018 AS (
  SELECT
    p.product_id,
    t.time_step,
    -- Predicted detrended sales = alpha + beta * time_step
    -- Reapply seasonality for each calendar month in 2018
    (r.alpha + r.beta * t.time_step) * sa.seasonality AS forecasted_sales
  FROM (
    SELECT DISTINCT product_id FROM sales_with_timestep
  ) p
  JOIN regression r ON p.product_id = r.product_id
  JOIN (
    -- time steps for 2018: Jan=25~Dec=36
    SELECT value AS time_step 
    FROM (SELECT 25 AS value UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30
          UNION ALL SELECT 31 UNION ALL SELECT 32 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35 UNION ALL SELECT 36)
  ) t
  LEFT JOIN (
    -- Map time_step to month number
    SELECT time_step, product_id, strftime('%m', month) AS month_num
    FROM sales_with_timestep
  ) ts_month
    ON p.product_id = ts_month.product_id AND t.time_step = ts_month.time_step
  LEFT JOIN seasonality_adj sa
    ON sa.product_id = p.product_id AND sa.month_num = ts_month.month_num
)
SELECT
  product_id,
  AVG(forecasted_sales) AS avg_forecasted_annual_sales_2018
FROM forecast_2018
GROUP BY product_id;
