WITH career_spans AS (
    SELECT ROUND(ABS(CAST(strftime('%Y', final_game) AS INTEGER) - CAST(strftime('%Y', debut) AS INTEGER)), 2)
         + ROUND(ABS(CAST(strftime('%m', final_game) AS INTEGER) - CAST(strftime('%m', debut) AS INTEGER)) / 12.0, 2)
         + ROUND(ABS(CAST(strftime('%d', final_game) AS INTEGER) - CAST(strftime('%d', debut) AS INTEGER)) / 365.0, 2) AS career_span_years
    FROM player
)
SELECT ROUND(AVG(career_span_years), 2) AS average_career_span_years
FROM career_spans;
