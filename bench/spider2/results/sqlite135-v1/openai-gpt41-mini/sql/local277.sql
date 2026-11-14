WITH monthly_data AS (
  SELECT
    product_id,
    strftime('%Y', order_date) AS year,
    strftime('%m', order_date) AS month,
    SUM(orderlines.sales_amount) AS sales_amount
  FROM orderlines
  JOIN orders ON orderlines.order_id = orders.order_id
  WHERE product_id IN (4160, 7790)
    AND order_date >= '2016-01-01' AND order_date < '2019-01-01'
  GROUP BY product_id, year, month
  ORDER BY product_id, year, month
),

monthly_data_extended AS (
  SELECT
    product_id,
    year,
    month,
    sales_amount,
    (CAST(year AS INTEGER) - 2016) * 12 + CAST(month AS INTEGER) AS time_step
  FROM monthly_data
),

cma_calculation AS (
  SELECT
    d1.product_id,
    d1.time_step,
    d1.sales_amount,
    (
      (SELECT AVG(d2.sales_amount) FROM monthly_data_extended d2 
       WHERE d2.product_id = d1.product_id AND d2.time_step BETWEEN d1.time_step-6 AND d1.time_step+5)
      +
      (SELECT AVG(d2.sales_amount) FROM monthly_data_extended d2 
       WHERE d2.product_id = d1.product_id AND d2.time_step BETWEEN d1.time_step-5 AND d1.time_step+6)
    ) / 2.0 AS cma
  FROM monthly_data_extended d1
  WHERE d1.time_step BETWEEN 7 AND 30
),

regression_coefficients AS (
  SELECT
    product_id,
    COUNT(*) AS n,
    SUM(time_step) AS sum_x,
    SUM(sales_amount) AS sum_y,
    SUM(time_step * sales_amount) AS sum_xy,
    SUM(time_step * time_step) AS sum_xx
  FROM monthly_data_extended
  WHERE time_step BETWEEN 1 AND 36
  GROUP BY product_id
),

regression_vals AS (
  SELECT
    product_id,
    ((n * sum_xy - sum_x * sum_y)*1.0) / (n * sum_xx - sum_x * sum_x) AS slope,
    ((sum_y - ((n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)) * sum_x)*1.0) / n AS intercept
  FROM regression_coefficients
),

forecast_2018 AS (
  SELECT
    r.product_id,
    t.time_step,
    r.slope * t.time_step + r.intercept AS forecasted_sales
  FROM regression_vals r
  JOIN (SELECT DISTINCT time_step FROM monthly_data_extended WHERE time_step BETWEEN 25 AND 36) t
),

seasonality_factors AS (
  SELECT
    product_id,
    season_month,
    avg_sales / avg_seasonal_sales AS seasonality_factor
  FROM (
    SELECT
      product_id,
      (time_step - 1) % 12 + 1 AS season_month,
      AVG(sales_amount) AS avg_sales
    FROM monthly_data_extended
    WHERE time_step BETWEEN 7 AND 30
    GROUP BY product_id, season_month
  ) ma
  JOIN (
    SELECT product_id, AVG(avg_sales) AS avg_seasonal_sales
    FROM (
      SELECT
        product_id,
        (time_step - 1) % 12 + 1 AS season_month,
        AVG(sales_amount) AS avg_sales
      FROM monthly_data_extended
      WHERE time_step BETWEEN 7 AND 30
      GROUP BY product_id, season_month
    )
    GROUP BY product_id
  ) pa ON ma.product_id = pa.product_id
),

forecast_with_seasonality AS (
  SELECT
    f.product_id,
    f.time_step,
    f.forecasted_sales,
    ((f.time_step - 1) % 12 + 1) AS season_month,
    s.seasonality_factor,
    f.forecasted_sales * s.seasonality_factor AS seasonality_adjusted_forecast
  FROM forecast_2018 f
  JOIN seasonality_factors s ON f.product_id = s.product_id AND ((f.time_step - 1) % 12 + 1) = s.season_month
  WHERE f.time_step BETWEEN 25 AND 36
),

annual_sales_2018 AS (
  SELECT
    product_id,
    SUM(seasonality_adjusted_forecast) AS annual_forecasted_sales_2018
  FROM forecast_with_seasonality
  GROUP BY product_id
)

SELECT AVG(annual_forecasted_sales_2018) AS average_forecasted_annual_sales_2018
FROM annual_sales_2018
WHERE product_id IN (4160, 7790);
