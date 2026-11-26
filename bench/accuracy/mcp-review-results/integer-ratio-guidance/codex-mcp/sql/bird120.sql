WITH badge_counts AS (
  SELECT
    COUNT(CASE WHEN strftime('%Y', b.Date) = '2010' THEN 1 END) AS count_2010,
    COUNT(CASE WHEN strftime('%Y', b.Date) = '2011' THEN 1 END) AS count_2011,
    COUNT(*) AS total
  FROM main.badges AS b
  WHERE b.Name = 'Student'
), ratio_input(group_id, numerator, denominator) AS (
  SELECT 2010, count_2010, total FROM badge_counts
  UNION ALL
  SELECT 2011, count_2011, total FROM badge_counts
), ratio_checked AS (
  SELECT group_id, numerator, denominator,
    CASE WHEN numerator IS NULL OR denominator IS NULL THEN 'null_input'
      WHEN typeof(numerator) NOT IN ('integer', 'real')
        OR typeof(denominator) NOT IN ('integer', 'real') THEN 'invalid_input'
      WHEN numerator NOT BETWEEN -90071992547409 AND 90071992547409
        OR denominator NOT BETWEEN -9007199254740991 AND 9007199254740991 THEN 'overflow'
      WHEN numerator <> CAST(numerator AS INTEGER)
        OR denominator <> CAST(denominator AS INTEGER) THEN 'invalid_input'
      WHEN denominator = 0 THEN 'zero_denominator'
      ELSE 'ok' END AS status
  FROM ratio_input
), ratio_result AS (
  SELECT group_id, numerator, denominator, status,
    CASE WHEN status = 'ok' THEN
      (CAST(numerator AS INTEGER) * 100) * 1.0 / CAST(denominator AS INTEGER)
    END AS ratio_value
  FROM ratio_checked
)
SELECT
  MAX(CASE WHEN group_id = 2010 THEN ratio_value END)
  - MAX(CASE WHEN group_id = 2011 THEN ratio_value END) AS difference_percentage_points
FROM ratio_result
LIMIT 1
