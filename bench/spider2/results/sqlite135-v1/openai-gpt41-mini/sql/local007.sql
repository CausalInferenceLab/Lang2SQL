WITH player_career_span AS (
  SELECT
    player_id,
    STRFTIME('%Y', final_game) - STRFTIME('%Y', debut) AS years_diff,
    STRFTIME('%m', final_game) - STRFTIME('%m', debut) AS months_diff,
    STRFTIME('%d', final_game) - STRFTIME('%d', debut) AS days_diff
  FROM player
  WHERE debut IS NOT NULL AND final_game IS NOT NULL
), career_span_calc AS (
  SELECT
    player_id,
    ROUND(ABS(years_diff), 2) AS years_part,
    ROUND(ABS(months_diff) / 12.0, 2) AS months_part,
    ROUND(ABS(days_diff) / 365.0, 2) AS days_part
  FROM player_career_span
), career_span_sum AS (
  SELECT
    player_id,
    years_part + months_part + days_part AS total_career_span
  FROM career_span_calc
)
SELECT ROUND(AVG(total_career_span), 2) AS avg_career_span_years
FROM career_span_sum;
