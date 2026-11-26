WITH player_spans AS (
    SELECT
        ROUND(ABS(strftime('%Y', final_game) - strftime('%Y', debut)), 2)
        + ROUND(ABS(strftime('%m', final_game) - strftime('%m', debut)) / 12.0, 2)
        + ROUND(ABS(strftime('%d', final_game) - strftime('%d', debut)) / 365.0, 2)
        AS career_span_years
    FROM main.player
)
SELECT ROUND(AVG(career_span_years), 2) AS average_career_span_years
FROM player_spans
LIMIT 1
