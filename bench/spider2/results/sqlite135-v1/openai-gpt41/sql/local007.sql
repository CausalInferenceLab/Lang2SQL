WITH career_span_parts AS (
  SELECT
    player_id,
    -- Calculate date parts
    debut,
    final_game,
    CAST(strftime('%Y', final_game) AS INTEGER) - CAST(strftime('%Y', debut) AS INTEGER) AS year_diff,
    CAST(strftime('%m', final_game) AS INTEGER) - CAST(strftime('%m', debut) AS INTEGER) AS month_diff,
    CAST(strftime('%d', final_game) AS INTEGER) - CAST(strftime('%d', debut) AS INTEGER) AS day_diff
  FROM player
  WHERE debut IS NOT NULL AND final_game IS NOT NULL
),
adjusted_span AS (
  SELECT
    player_id,
    -- Adjust month and day differences: "borrow" from months and years as needed
    CASE 
      WHEN month_diff < 0 THEN year_diff - 1
      ELSE year_diff
    END AS adj_year,
    ROUND(
      CASE 
        WHEN month_diff < 0 THEN month_diff + 12
        ELSE month_diff
      END
    , 2) AS adj_month,
    ROUND(
      CASE 
        WHEN day_diff < 0 THEN
          -- Reduce adjusted month by 1 and add days-in-previous-month (approximate 30)
          day_diff + 30
        ELSE day_diff
      END
    , 2) AS adj_day
  FROM career_span_parts
),
career_span AS (
  SELECT
    player_id,
    ROUND(ABS(adj_year), 2) AS rd_year,
    ROUND(ABS(adj_month) / 12.0, 2) AS rd_month,
    ROUND(ABS(adj_day) / 365.0, 2) AS rd_day,
    ROUND(
      ROUND(ABS(adj_year), 2) + ROUND(ABS(adj_month) / 12.0, 2) + ROUND(ABS(adj_day) / 365.0, 2)
    , 6) AS career_span_yr
  FROM adjusted_span
)
SELECT
  ROUND(AVG(career_span_yr), 6) AS avg_career_span_years
FROM career_span;
